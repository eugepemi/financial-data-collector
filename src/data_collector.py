import websocket
import json
from storage_ingestion import AzureBlobIngestor

# Instantiating the Azure Blob ingestor
ingestor = AzureBlobIngestor()

def on_open(ws):
    """
    Callback executed at socket opening
    """
    print("Connection opened")
    subscribe_message = {
        "type": "subscribe",
        "channels": [
            {
                "name": "ticker",
                "product_ids": [
                    # TODO: Hardcoded value, change to a variable
                    "BTC-USD"       
                ]
            }
        ]
    }
    ws.send(json.dumps(subscribe_message))

def on_message(ws, message):
    """
    Function to be executed when a message is received from the WebSocket.
    """
    print("Message received")
    data = json.loads(message)
    print(data)
    # Ingest the data into Azure Blob Storage
    ingestor.store_data(data)

# WebSocket URL for Coinbase Pro
socket = "wss://ws-feed.pro.coinbase.com"

# Instantiate the WebSocketApp and set the callback functions
ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message)

# Start the WebSocket connection and run forever
ws.run_forever()
