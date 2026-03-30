import websocket
import json
import csv
import os
import time
import dotenv
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

API_KEY = os.getenv("REAL_API_KEY")

SYMBOLS = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "BINANCE:BNBUSDT"
]

# ================= FILE SETUP =================

def init_csv(file_name, header):
    # Dùng "w" để ghi đè file cũ, đảm bảo luôn có tên cột mỗi khi chạy code
    f = open(file_name, "w", newline="", buffering=1)
    writer = csv.writer(f)
    writer.writerow(header)
    return writer


trade_writer = init_csv("fact_trades.csv", [
    "trade_id", "symbol_id", "date_id", "time_id",
    "exchange_id", "price", "volume",
    "notional_value", "is_block_trade", "event_ts"
])

quote_writer = init_csv("fact_quote.csv", [
    "quote_id", "symbol_id", "date_id", "time_id",
    "exchange_id", "bid_price", "ask_price",
    "bid_size", "ask_size", "spread",
    "mid_price", "event_ts"
])

trade_id = 1
quote_id = 1


# ================= UTIL =================

def extract_time_fields(timestamp):
    dt = datetime.fromtimestamp(timestamp / 1000)

    date_id = int(dt.strftime("%Y%m%d"))
    time_id = int(dt.strftime("%H%M%S"))

    return dt, date_id, time_id


def extract_exchange(symbol):
    return symbol.split(":")[0]


# ================= SAVE FUNCTIONS =================

def save_trade(trade):
    global trade_id

    symbol = trade.get("s")
    price = trade.get("p")
    volume = trade.get("v")
    timestamp = trade.get("t")

    if not symbol or not price or not volume or not timestamp:
        return

    dt, date_id, time_id = extract_time_fields(timestamp)
    exchange = extract_exchange(symbol)

    notional = price * volume

    trade_writer.writerow([
        trade_id, symbol, date_id, time_id,
        exchange, price, volume, notional, False, dt
    ])
    
    print(f"👉 [TRADE] {symbol} | Giá: {price} | KL: {volume}")

    trade_id += 1


def save_quote(q):
    global quote_id

    symbol = q.get("s")
    bid = q.get("b")
    ask = q.get("a")
    bid_size = q.get("bs")
    ask_size = q.get("as")
    timestamp = q.get("t")

    if not symbol or not bid or not ask or not timestamp:
        return

    dt, date_id, time_id = extract_time_fields(timestamp)
    exchange = extract_exchange(symbol)

    spread = ask - bid
    mid_price = (ask + bid) / 2

    quote_writer.writerow([
        quote_id, symbol, date_id, time_id, exchange,
        bid, ask, bid_size, ask_size, spread, mid_price, dt
    ])
    
    print(f"📊 [QUOTE] {symbol} | Mua: {bid} | Bán: {ask}")

    quote_id += 1


# ================= WEBSOCKET =================

def on_message(ws, message):
    try:
        msg = json.loads(message)

        # Handle Binance Trade Event
        if msg.get("e") == "trade":
            trade_data = {
                "s": f"BINANCE:{msg['s']}",
                "p": float(msg['p']),
                "v": float(msg['q']),
                "t": msg['E']
            }
            save_trade(trade_data)

        # Handle Binance Quote Event (bookTicker)
        # bookTicker events don't have an "e" field, but have fields like "u", "s", "b", "a"
        elif "u" in msg and "b" in msg and "a" in msg:
            quote_data = {
                "s": f"BINANCE:{msg['s']}",
                "b": float(msg['b']),
                "a": float(msg['a']),
                "bs": float(msg['B']),
                "as": float(msg['A']),
                "t": int(time.time() * 1000)  # Add timestamp manually
            }
            save_quote(quote_data)

    except Exception as e:
        pass # Ignore parsing errors for system messages like ping/pong


def on_open(ws):
    print("Connected to Binance")

    params = []
    for symbol in SYMBOLS:
        # Convert "BINANCE:BTCUSDT" -> "btcusdt"
        raw_symbol = symbol.split(":")[1].lower()
        params.append(f"{raw_symbol}@trade")
        params.append(f"{raw_symbol}@bookTicker")

    # Send subscription to Binance
    ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": params,
        "id": 1
    }))


def on_error(ws, error):
    print("ERROR:", error)


def on_close(ws, close_status_code, close_msg):
    print("Disconnected")


# ================= MAIN =================

if __name__ == "__main__":
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://stream.binance.com:9443/ws",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever()

        except Exception as e:
            print("Reconnect error:", e)
            time.sleep(5)