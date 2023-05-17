import json
from storage_ingestion import AzureBlobStorage
import asyncio
import websockets

ingestor = AzureBlobStorage()

async def subscribe_ticker(ticker):
    """
    This function is an asynchronous function that connects to a WebSocket, subscribes to a specific ticker, 
    and stores the received messages in Azure Blob Storage.

    Args:
        ticker (str): The ticker to subscribe to
    """
    # Connect to the WebSocket
    async with websockets.connect('wss://ws-feed.pro.coinbase.com') as ws:
        # Define the subscription message
        subscribe_message = {
            "type": "subscribe",
            "channels": [
                {
                    "name": "ticker",
                    "product_ids": [
                        ticker
                    ]
                }
            ]
        }
        # Send the subscription message
        await ws.send(json.dumps(subscribe_message))

        batch_data = []
        batch_size = 5000

        # Process received messages
        async for message in ws:
            data = json.loads(message)
            # If the message includes a 'product_id' field, store it in Azure Blob Storage
            if 'product_id' in data:
                batch_data.append(data)

                if len(batch_data) >= batch_size:
                    ingestor.insert_data(batch_data, "bronze")
                    # Empty the batch_data list for the next batch
                    batch_data = []


# Define the list of tickers to subscribe to
tickers = ["BTC-USD", "ETH-USD", "USDT-USD", "BNB-USD", "XRP-USD", "DOGE-USD"]  # TODO: avoid hardcoding

async def gather_tasks():
    """
    This function creates a list of tasks, each of which is a coroutine for subscribing to a specific ticker.
    with the objective to run all these tasks concurrently.
    """
    # Create a list of tasks
    tasks = []
    for ticker in tickers:
        # Each task is a subscribe_ticker coroutine for each ticker
        task = subscribe_ticker(ticker)
        tasks.append(task)
    
    # Run all tasks concurrently
    await asyncio.gather(*tasks)

# Run the main function
asyncio.run(gather_tasks())
