"""Compatibility module; use ``bin/trade mcp`` as the stable entrypoint."""
from astock_trading.platform.mcp_server import main

if __name__ == "__main__":
    main()
