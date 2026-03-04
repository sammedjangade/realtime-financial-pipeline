import sys
sys.path.append('.')
import websocket
import json
import boto3
from datetime import datetime
from config import AWS_REGION, KINESIS_STREAM_NAME

kinesis = boto3.client('kinesis', region_name=AWS_REGION)

def enrich_event(data):
    price = float(data['price'])
    size = float(data['size'])
    trade_value = price * size

    if trade_value >= 10000:
        size_category = 'WHALE'
    elif trade_value >= 1000:
        size_category = 'LARGE'
    elif trade_value >= 100:
        size_category = 'MEDIUM'
    else:
        size_category = 'SMALL'

    return {
        'trade_id': data['trade_id'],
        'product_id': data['product_id'],
        'price': price,
        'size': size,
        'side': data['side'],
        'trade_timestamp': data['time'],
        'ingested_at': datetime.utcnow().isoformat(),
        'trade_value_usd': round(trade_value, 2),
        'size_category': size_category
    }

def validate_event(event):
    errors = []

    if not event.get('trade_id'):
        errors.append('missing trade_id')
    if not event.get('product_id'):
        errors.append('missing product_id')
    if event.get('price') <= 0:
        errors.append(f"invalid price: {event.get('price')}")
    if event.get('size') <= 0:
        errors.append(f"invalid size: {event.get('size')}")
    if event.get('side') not in ['buy', 'sell']:
        errors.append(f"invalid side: {event.get('side')}")
    if event.get('trade_value_usd') < 0:
        errors.append(f"negative trade value: {event.get('trade_value_usd')}")

    return errors

def on_message(ws, message):
    data = json.loads(message)

    if data.get('type') == 'match':
        event = enrich_event(data)

        errors = validate_event(event)
        if errors:
            print(f"❌ Validation failed for {event.get('trade_id')}: {errors}")
            return

        kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(event),
            PartitionKey=event['product_id']
        )
        print(f"✅ {event['product_id']} | {event['side'].upper()} | ${event['price']} | {event['size_category']}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD"],
        "channels": ["matches"]
    }
    ws.send(json.dumps(subscribe_msg))
    print("Connected to Coinbase WebSocket")

if __name__ == '__main__':
    ws = websocket.WebSocketApp(
        "wss://ws-feed.exchange.coinbase.com",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()