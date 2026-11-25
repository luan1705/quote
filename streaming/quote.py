import json
import logging
from dataclasses import asdict

from ssi_sdk import Auth, Stream
from streaming.config import SSI_CONFIG, CHANNEL_ASSET, REDIS
from streaming.list import HOSE, HNX, UPCOM, BOND

PRICE_DIV_1000_SYMBOLS = set(HOSE + HNX + UPCOM + BOND)

logger = logging.getLogger(__name__)


def format_message(data):
    symbol = data.get("symbol")
    trading_time = data.get("trading_time")

    if not symbol or not trading_time:
        return None

    time = str(trading_time)[:19].replace("/", "-")

    bid_prices = data.get("bid_prices") or []
    bid_volumes = data.get("bid_volumes") or []
    ask_prices = data.get("ask_prices") or []
    ask_volumes = data.get("ask_volumes") or []

    def price(v):
        if v is None:
            return None
        return v / 1000 if symbol in PRICE_DIV_1000_SYMBOLS else v

    result = {
        "symbol": symbol,
        "time": time,
        "buyPrice1": price(bid_prices[0]) if len(bid_prices) > 0 else None,
        "buyVol1": bid_volumes[0] if len(bid_volumes) > 0 else None,
        "buyPrice2": price(bid_prices[1]) if len(bid_prices) > 1 else None,
        "buyVol2": bid_volumes[1] if len(bid_volumes) > 1 else None,
        "buyPrice3": price(bid_prices[2]) if len(bid_prices) > 2 else None,
        "buyVol3": bid_volumes[2] if len(bid_volumes) > 2 else None,
        "sellPrice1": price(ask_prices[0]) if len(ask_prices) > 0 else None,
        "sellVol1": ask_volumes[0] if len(ask_volumes) > 0 else None,
        "sellPrice2": price(ask_prices[1]) if len(ask_prices) > 1 else None,
        "sellVol2": ask_volumes[1] if len(ask_volumes) > 1 else None,
        "sellPrice3": price(ask_prices[2]) if len(ask_prices) > 2 else None,
        "sellVol3": ask_volumes[2] if len(ask_volumes) > 2 else None,
    }

    return {k: v for k, v in result.items() if v is not None}


def on_market_data(msg):
    try:
        if isinstance(msg, dict):
            data = msg
        else:
            data = asdict(msg)
            if hasattr(msg, "type"):
                data["type"] = msg.type.value if hasattr(msg.type, "value") else msg.type

        quote = format_message(data)
        if quote:
            payload = json.dumps(quote, ensure_ascii=False)
            REDIS.publish(CHANNEL_ASSET, payload)
            REDIS.hset("quote", quote["symbol"], payload)
    except Exception:
        logger.exception("Quote message error")


def stream_symbol_quote(symbols):
    with Auth(SSI_CONFIG) as auth:
        auth.authenticate()
        with Stream(auth) as stream:
            stream.streaming.on_data = on_market_data
            stream.streaming.connect()
            stream.streaming.subscribe_symbol_quote(symbols)
            stream.streaming.wait()
