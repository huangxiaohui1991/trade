"""MX CLI encapsulation helpers.

This module provides a small registry/dispatcher layer over the existing
`scripts.mx` capability modules. It does not execute any CLI parsing by itself;
instead it exposes structured command metadata and callable runners that the
main trading thread can later wire into `trade.py`.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple


def _normalize_command_name(name: str) -> str:
    return " ".join(str(name).strip().lower().split())


def _load_command_class(module_name: str, class_name: str) -> Tuple[Optional[type], Optional[str]]:
    """Load a command client class lazily.

    Returns a pair of `(class, error_message)`. The loader is intentionally
    permissive so the registry can include capability metadata even when an
    optional MX module is missing.
    """

    try:
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls, None
    except Exception as exc:  # pragma: no cover - exercised through availability tests
        return None, str(exc)


@dataclass(frozen=True)
class CommandArgSpec:
    name: str
    type: str
    required: bool = True
    help: str = ""
    default: Any = None
    choices: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "name": self.name,
            "type": self.type,
            "required": self.required,
            "help": self.help,
        }
        if self.default is not None:
            payload["default"] = self.default
        if self.choices:
            payload["choices"] = list(self.choices)
        return payload


@dataclass(frozen=True)
class CommandSpec:
    id: str
    title: str
    summary: str
    group: str
    aliases: Tuple[str, ...] = ()
    args: Tuple[CommandArgSpec, ...] = ()
    available: bool = True
    availability_note: str = ""
    source_module: str = ""
    source_class: str = ""
    runner: Callable[..., Any] = field(repr=False, compare=False, default=lambda **_: None)

    def keys(self) -> Tuple[str, ...]:
        return tuple(dict.fromkeys((_normalize_command_name(self.id),) + tuple(_normalize_command_name(a) for a in self.aliases)))

    def to_dict(self, include_runner: bool = False) -> Dict[str, Any]:
        payload = {
            "id": self.id,
            "title": self.title,
            "summary": self.summary,
            "group": self.group,
            "aliases": list(self.aliases),
            "args": [arg.to_dict() for arg in self.args],
            "available": self.available,
            "availability_note": self.availability_note,
            "source_module": self.source_module,
            "source_class": self.source_class,
        }
        if include_runner:
            payload["runner"] = self.runner
        return payload


class MXCommandError(RuntimeError):
    pass


class MXCommandNotFound(MXCommandError):
    pass


class MXCommandUnavailable(MXCommandError):
    pass


def _ensure_client(client: Any, client_cls: Optional[type]) -> Any:
    if client is not None:
        return client
    if client_cls is None:
        raise MXCommandUnavailable("MX command client is unavailable")
    return client_cls()


def _unavailable_runner(command_id: str, note: str) -> Callable[..., Any]:
    def runner(**_: Any) -> Any:
        raise MXCommandUnavailable(f"{command_id} unavailable: {note}")

    return runner


def _make_method_runner(method_name: str, client_cls: Optional[type]) -> Callable[..., Any]:
    def runner(query: str, *, client: Any = None) -> Any:
        instance = _ensure_client(client, client_cls)
        return getattr(instance, method_name)(query)

    return runner


def _make_noarg_runner(method_name: str, client_cls: Optional[type]) -> Callable[..., Any]:
    def runner(*, client: Any = None) -> Any:
        instance = _ensure_client(client, client_cls)
        return getattr(instance, method_name)()

    return runner


def _make_manage_runner(client_cls: Optional[type]) -> Callable[..., Any]:
    def runner(query: str, *, client: Any = None) -> Any:
        instance = _ensure_client(client, client_cls)
        return instance.manage(query)

    return runner


def _make_trade_runner(client_cls: Optional[type], side: str) -> Callable[..., Any]:
    def runner(
        stock_code: str,
        quantity: int,
        price: Optional[float] = None,
        use_market_price: bool = False,
        *,
        client: Any = None,
    ) -> Any:
        instance = _ensure_client(client, client_cls)
        return instance.trade(side, stock_code, quantity, price, use_market_price)

    return runner


def _make_cancel_runner(client_cls: Optional[type], cancel_all_default: bool = False) -> Callable[..., Any]:
    def runner(
        order_id: Optional[str] = None,
        cancel_all: bool = False,
        *,
        client: Any = None,
    ) -> Any:
        instance = _ensure_client(client, client_cls)
        if cancel_all or cancel_all_default:
            return instance.cancel(cancel_all=True)
        return instance.cancel(order_id=order_id)

    return runner


def _build_specs(include_unavailable: bool = False) -> List[CommandSpec]:
    client_specs = {
        "mx.data.query": ("scripts.mx.mx_data", "MXData"),
        "mx.search.news": ("scripts.mx.mx_search", "MXSearch"),
        "mx.xuangu.search": ("scripts.mx.mx_xuangu", "MXXuangu"),
        "mx.zixuan.query": ("scripts.mx.mx_zixuan", "MXZixuan"),
        "mx.zixuan.manage": ("scripts.mx.mx_zixuan", "MXZixuan"),
        "mx.moni.positions": ("scripts.mx.mx_moni", "MXMoni"),
        "mx.moni.balance": ("scripts.mx.mx_moni", "MXMoni"),
        "mx.moni.orders": ("scripts.mx.mx_moni", "MXMoni"),
        "mx.moni.buy": ("scripts.mx.mx_moni", "MXMoni"),
        "mx.moni.sell": ("scripts.mx.mx_moni", "MXMoni"),
        "mx.moni.cancel": ("scripts.mx.mx_moni", "MXMoni"),
        "mx.moni.cancel_all": ("scripts.mx.mx_moni", "MXMoni"),
    }

    resolved: Dict[str, Tuple[Optional[type], Optional[str]]] = {
        command_id: _load_command_class(module_name, class_name)
        for command_id, (module_name, class_name) in client_specs.items()
    }

    def loaded(command_id: str) -> Tuple[Optional[type], Optional[str]]:
        return resolved[command_id]

    specs: List[CommandSpec] = []

    def add_spec(spec: CommandSpec) -> None:
        if spec.available or include_unavailable:
            specs.append(spec)

    data_cls, data_err = loaded("mx.data.query")
    add_spec(
        CommandSpec(
            id="mx.data.query",
            title="金融数据查询",
            summary="自然语言查询行情、财务和关系类数据。",
            group="data",
            aliases=("data.query", "data"),
            args=(CommandArgSpec("query", "string", True, "自然语言查询问句"),),
            available=data_cls is not None,
            availability_note=data_err or "",
            source_module="scripts.mx.mx_data",
            source_class="MXData",
            runner=_make_method_runner("query", data_cls) if data_cls is not None else _unavailable_runner("mx.data.query", data_err or "module unavailable"),
        )
    )

    search_cls, search_err = loaded("mx.search.news")
    add_spec(
        CommandSpec(
            id="mx.search.news",
            title="资讯搜索",
            summary="搜索研报、新闻和公告类金融资讯。",
            group="search",
            aliases=("search.news", "news"),
            args=(CommandArgSpec("query", "string", True, "资讯搜索问句"),),
            available=search_cls is not None,
            availability_note=search_err or "",
            source_module="scripts.mx.mx_search",
            source_class="MXSearch",
            runner=_make_method_runner("search", search_cls) if search_cls is not None else _unavailable_runner("mx.search.news", search_err or "module unavailable"),
        )
    )

    xuangu_cls, xuangu_err = loaded("mx.xuangu.search")
    add_spec(
        CommandSpec(
            id="mx.xuangu.search",
            title="智能选股",
            summary="按自然语言条件筛选股票池。",
            group="xuangu",
            aliases=("xuangu.search", "xuangu"),
            args=(CommandArgSpec("query", "string", True, "选股条件问句"),),
            available=xuangu_cls is not None,
            availability_note=xuangu_err or "",
            source_module="scripts.mx.mx_xuangu",
            source_class="MXXuangu",
            runner=_make_method_runner("search", xuangu_cls) if xuangu_cls is not None else _unavailable_runner("mx.xuangu.search", xuangu_err or "module unavailable"),
        )
    )

    zixuan_cls, zixuan_err = loaded("mx.zixuan.query")
    add_spec(
        CommandSpec(
            id="mx.zixuan.query",
            title="自选股查询",
            summary="读取东方财富自选股列表。",
            group="zixuan",
            aliases=("zixuan.query", "zixuan"),
            args=(),
            available=zixuan_cls is not None,
            availability_note=zixuan_err or "",
            source_module="scripts.mx.mx_zixuan",
            source_class="MXZixuan",
            runner=_make_noarg_runner("query", zixuan_cls) if zixuan_cls is not None else _unavailable_runner("mx.zixuan.query", zixuan_err or "module unavailable"),
        )
    )
    add_spec(
        CommandSpec(
            id="mx.zixuan.manage",
            title="自选股管理",
            summary="通过自然语言管理自选股的增删。",
            group="zixuan",
            aliases=("zixuan.manage",),
            args=(CommandArgSpec("query", "string", True, "管理问句或操作描述"),),
            available=zixuan_cls is not None,
            availability_note=zixuan_err or "",
            source_module="scripts.mx.mx_zixuan",
            source_class="MXZixuan",
            runner=_make_manage_runner(zixuan_cls) if zixuan_cls is not None else _unavailable_runner("mx.zixuan.manage", zixuan_err or "module unavailable"),
        )
    )

    moni_cls, moni_err = loaded("mx.moni.positions")
    add_spec(
        CommandSpec(
            id="mx.moni.positions",
            title="模拟交易持仓",
            summary="查询模拟组合持仓。",
            group="moni",
            aliases=("moni.positions",),
            args=(),
            available=moni_cls is not None,
            availability_note=moni_err or "",
            source_module="scripts.mx.mx_moni",
            source_class="MXMoni",
            runner=_make_noarg_runner("positions", moni_cls) if moni_cls is not None else _unavailable_runner("mx.moni.positions", moni_err or "module unavailable"),
        )
    )
    add_spec(
        CommandSpec(
            id="mx.moni.balance",
            title="模拟交易资金",
            summary="查询模拟组合资金余额。",
            group="moni",
            aliases=("moni.balance",),
            args=(),
            available=moni_cls is not None,
            availability_note=moni_err or "",
            source_module="scripts.mx.mx_moni",
            source_class="MXMoni",
            runner=_make_noarg_runner("balance", moni_cls) if moni_cls is not None else _unavailable_runner("mx.moni.balance", moni_err or "module unavailable"),
        )
    )
    add_spec(
        CommandSpec(
            id="mx.moni.orders",
            title="模拟交易委托",
            summary="查询模拟组合委托。",
            group="moni",
            aliases=("moni.orders",),
            args=(),
            available=moni_cls is not None,
            availability_note=moni_err or "",
            source_module="scripts.mx.mx_moni",
            source_class="MXMoni",
            runner=_make_noarg_runner("orders", moni_cls) if moni_cls is not None else _unavailable_runner("mx.moni.orders", moni_err or "module unavailable"),
        )
    )
    add_spec(
        CommandSpec(
            id="mx.moni.buy",
            title="模拟买入",
            summary="提交模拟组合买入指令。",
            group="moni",
            aliases=("moni.buy",),
            args=(
                CommandArgSpec("stock_code", "string", True, "6位股票代码"),
                CommandArgSpec("quantity", "integer", True, "委托数量，股数"),
                CommandArgSpec("price", "number", False, "限价价格"),
                CommandArgSpec("use_market_price", "boolean", False, "是否使用市价", default=False),
            ),
            available=moni_cls is not None,
            availability_note=moni_err or "",
            source_module="scripts.mx.mx_moni",
            source_class="MXMoni",
            runner=_make_trade_runner(moni_cls, "buy") if moni_cls is not None else _unavailable_runner("mx.moni.buy", moni_err or "module unavailable"),
        )
    )
    add_spec(
        CommandSpec(
            id="mx.moni.sell",
            title="模拟卖出",
            summary="提交模拟组合卖出指令。",
            group="moni",
            aliases=("moni.sell",),
            args=(
                CommandArgSpec("stock_code", "string", True, "6位股票代码"),
                CommandArgSpec("quantity", "integer", True, "委托数量，股数"),
                CommandArgSpec("price", "number", False, "限价价格"),
                CommandArgSpec("use_market_price", "boolean", False, "是否使用市价", default=False),
            ),
            available=moni_cls is not None,
            availability_note=moni_err or "",
            source_module="scripts.mx.mx_moni",
            source_class="MXMoni",
            runner=_make_trade_runner(moni_cls, "sell") if moni_cls is not None else _unavailable_runner("mx.moni.sell", moni_err or "module unavailable"),
        )
    )
    add_spec(
        CommandSpec(
            id="mx.moni.cancel",
            title="模拟撤单",
            summary="撤销单笔模拟委托。",
            group="moni",
            aliases=("moni.cancel",),
            args=(
                CommandArgSpec("order_id", "string", False, "委托编号"),
                CommandArgSpec("cancel_all", "boolean", False, "是否一键撤单", default=False),
            ),
            available=moni_cls is not None,
            availability_note=moni_err or "",
            source_module="scripts.mx.mx_moni",
            source_class="MXMoni",
            runner=_make_cancel_runner(moni_cls) if moni_cls is not None else _unavailable_runner("mx.moni.cancel", moni_err or "module unavailable"),
        )
    )
    add_spec(
        CommandSpec(
            id="mx.moni.cancel_all",
            title="模拟一键撤单",
            summary="撤销全部模拟委托。",
            group="moni",
            aliases=("moni.cancel_all", "moni.cancel-all"),
            args=(),
            available=moni_cls is not None,
            availability_note=moni_err or "",
            source_module="scripts.mx.mx_moni",
            source_class="MXMoni",
            runner=_make_cancel_runner(moni_cls, cancel_all_default=True) if moni_cls is not None else _unavailable_runner("mx.moni.cancel_all", moni_err or "module unavailable"),
        )
    )

    return specs


class MXCommandRegistry:
    def __init__(self, specs: Iterable[CommandSpec]):
        self._specs: Tuple[CommandSpec, ...] = tuple(specs)
        self._lookup: Dict[str, CommandSpec] = {}
        for spec in self._specs:
            for key in spec.keys():
                if key in self._lookup:
                    other = self._lookup[key].id
                    raise ValueError(f"duplicate MX command alias: {key} (already mapped to {other})")
                self._lookup[key] = spec

    def specs(self, include_unavailable: bool = True) -> Tuple[CommandSpec, ...]:
        if include_unavailable:
            return self._specs
        return tuple(spec for spec in self._specs if spec.available)

    def metadata(self, include_unavailable: bool = True) -> List[Dict[str, Any]]:
        return [spec.to_dict(include_runner=False) for spec in self.specs(include_unavailable=include_unavailable)]

    def dispatch_table(self, include_unavailable: bool = True) -> Dict[str, Callable[..., Any]]:
        return {spec.id: spec.runner for spec in self.specs(include_unavailable=include_unavailable)}

    def resolve(self, command: str) -> CommandSpec:
        key = _normalize_command_name(command)
        spec = self._lookup.get(key)
        if spec is None:
            raise MXCommandNotFound(f"unknown MX command: {command}")
        return spec

    def dispatch(self, command: str, **kwargs: Any) -> Any:
        spec = self.resolve(command)
        if not spec.available:
            raise MXCommandUnavailable(spec.availability_note or f"{spec.id} unavailable")
        return spec.runner(**kwargs)

    def by_group(self, include_unavailable: bool = True) -> Dict[str, List[CommandSpec]]:
        grouped: Dict[str, List[CommandSpec]] = {}
        for spec in self.specs(include_unavailable=include_unavailable):
            grouped.setdefault(spec.group, []).append(spec)
        return grouped


def build_mx_command_registry(include_unavailable: bool = False) -> MXCommandRegistry:
    return MXCommandRegistry(_build_specs(include_unavailable=include_unavailable))


def list_mx_command_metadata(include_unavailable: bool = False) -> List[Dict[str, Any]]:
    return build_mx_command_registry(include_unavailable=include_unavailable).metadata(include_unavailable=True)


def get_mx_command_spec(command: str, include_unavailable: bool = True) -> CommandSpec:
    registry = build_mx_command_registry(include_unavailable=include_unavailable)
    return registry.resolve(command)


def dispatch_mx_command(command: str, **kwargs: Any) -> Any:
    registry = build_mx_command_registry(include_unavailable=True)
    return registry.dispatch(command, **kwargs)


def mx_dispatch_table(include_unavailable: bool = False) -> Dict[str, Callable[..., Any]]:
    return build_mx_command_registry(include_unavailable=include_unavailable).dispatch_table(include_unavailable=include_unavailable)


def mx_command_groups(include_unavailable: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    registry = build_mx_command_registry(include_unavailable=include_unavailable)
    grouped = registry.by_group(include_unavailable=include_unavailable)
    return {group: [spec.to_dict() for spec in specs] for group, specs in grouped.items()}
