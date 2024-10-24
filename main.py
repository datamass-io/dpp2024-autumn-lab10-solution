import asyncio
import json
import os
import psutil
import random
import time
import uuid
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = "mdevents"


def get_size(bytes_data):
    for unit in ['', 'K', 'M', 'G', 'T']:
        if bytes_data < 1024:
            return f"{bytes_data:.2f}{unit}B"
        bytes_data /= 1024


async def send_event(producer):
    update_delay = 5
    old_io = psutil.net_io_counters()
    while True:
        # Gather system metrics
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
        cpu = psutil.cpu_percent()
        mem = psutil.virtual_memory().percent
        swap = psutil.swap_memory().percent
        process_count = sum(1 for _ in psutil.process_iter())
        new_io = psutil.net_io_counters()
        download_speed_bytes = (new_io.bytes_recv - old_io.bytes_recv) / update_delay
        download_speed = get_size(download_speed_bytes)
        upload_speed_bytes = (new_io.bytes_sent - old_io.bytes_sent) / update_delay
        upload_speed = get_size(upload_speed_bytes)
        old_io = new_io

        # Prepare the data to send
        data = {
            "id": str(uuid.uuid4()),
            "system": None if random.random() < 0.1 else "home",
            "timestamp": timestamp,
            "cpu_usage": 100 * cpu if random.random() < 0.1 else cpu,
            "mem_usage": 100 * cpu if random.random() < 0.1 else cpu,
            "swap_usage": swap,
            "process_count": process_count,
            "download_speed": download_speed,
            "download_speed_bytes": -1.0 * download_speed_bytes if random.random() < 0.1 else download_speed_bytes,
            "upload_speed": upload_speed,
            "upload_speed_bytes": -1.0 * upload_speed_bytes if random.random() < 0.1 else upload_speed_bytes,
        }

        # Create a batch of events to send
        event_data_batch = await producer.create_batch()

        # Add events to the batch
        event_data_batch.add(EventData(json.dumps(data)))

        # Send the batch of events to the event hub
        await producer.send_batch(event_data_batch)

        # Print the message sent for logging purposes
        print(f"Sent event: {data}")

        # Wait for update_delay seconds before sending the next event
        await asyncio.sleep(update_delay)

async def run():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    async with producer:
        await send_event(producer)


if __name__ == "__main__":
    asyncio.run(run())
