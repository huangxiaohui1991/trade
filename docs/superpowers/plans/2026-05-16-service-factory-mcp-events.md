# Service Factory MCP Events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce architectural drift by centralizing service construction, thinning MCP runtime code, and adding named event contracts for high-value workflow events.

**Architecture:** Add `astock_trading.platform.service_factory` as the composition root used by pipeline and MCP entrypoints. Keep public CLI/MCP commands stable while moving shared initialization and event names behind focused modules.

**Tech Stack:** Python 3.12, Typer/FastMCP wrappers, existing EventStore/MySQL-compatible repository, pytest, ruff.

---

### Task 1: Service Composition Root

**Files:**
- Create: `src/astock_trading/platform/service_factory.py`
- Modify: `src/astock_trading/pipeline/context.py`
- Modify: `src/astock_trading/platform/mcp_server.py`
- Test: `tests/astock_trading/platform/test_service_factory.py`

- [ ] **Step 1: Write failing provider parity tests**

Add tests that assert `build_market_service(conn)` includes `AkShareHKMarketAdapter` in market providers and `AkShareHKFinancialAdapter` in financial providers, and that `build_context()` uses the same builder.

- [ ] **Step 2: Run focused tests**

Run: `uv run pytest tests/astock_trading/platform/test_service_factory.py -q`
Expected: FAIL because `astock_trading.platform.service_factory` does not exist yet.

- [ ] **Step 3: Implement `service_factory`**

Create helper functions for vault path resolution, trade hook construction, market service construction, strategy service construction, and full runtime service construction.

- [ ] **Step 4: Wire existing entrypoints**

Update pipeline context and MCP lazy initialization to call the shared factory rather than duplicating provider lists.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/astock_trading/platform/test_service_factory.py tests/astock_trading/pipeline/test_pipelines.py tests/astock_trading/platform/test_mcp_tools.py -q`
Expected: PASS.

### Task 2: Event Contracts

**Files:**
- Create: `src/astock_trading/platform/domain_events.py`
- Modify: `src/astock_trading/strategy/service.py`
- Modify: `src/astock_trading/pipeline/auto_trade.py`
- Modify: `src/astock_trading/platform/mcp_server.py`
- Test: `tests/astock_trading/platform/test_domain_events.py`

- [ ] **Step 1: Write failing event contract tests**

Add tests for named constants such as `SCORE_CALCULATED`, `DECISION_SUGGESTED`, `MANUAL_TRADE_REQUESTED`, `AUTO_TRADE_EXECUTED`, and a `DomainEventPublisher` wrapper.

- [ ] **Step 2: Implement the event contract module**

Create constants and a small publisher that forwards structured event fields to `EventStore.append()`.

- [ ] **Step 3: Migrate high-value event writes**

Use constants and the publisher in strategy decision events, manual trade request events, candidate/MCP events, and auto-trade paper execution events.

- [ ] **Step 4: Verify event behavior**

Run focused strategy/auto-trade/MCP tests and confirm event type strings remain unchanged.

### Task 3: Final Verification

**Files:**
- No additional production files.

- [ ] **Step 1: Run static checks**

Run: `uv run ruff check .`
Expected: PASS.

- [ ] **Step 2: Run full test suite**

Run: `uv run pytest -q`
Expected: PASS.

- [ ] **Step 3: Run operational health entrypoint**

Run: `bin/trade doctor --json`
Expected: JSON payload with `"status": "ok"` or a clear existing environment/data issue unrelated to the refactor.
