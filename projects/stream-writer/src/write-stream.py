import json
import os
import uuid
import time
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential

# Azure Event Hub details
EVENT_HUB_NAMESPACE = "pricing-streaming"
EVENT_HUB_NAME = "streaming-input"

# Authenticate using Azure AD
credential = DefaultAzureCredential()

# Get current script directory and construct paths
current_dir = os.path.dirname(os.path.abspath(__file__))
base_folder = os.path.dirname(current_dir)
sku_folder_path = os.path.join(base_folder, "data", "sku")
suitability_folder_path = os.path.join(base_folder, "data", "suitability")

def read_json_file(file_path):
    """Reads a JSON file and returns its content."""
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            return json.load(file)
    return None  # Return None if file does not exist

# Function to send messages to Event Hub
def send_to_event_hub(data):
    producer = EventHubProducerClient(
        fully_qualified_namespace=f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net",
        eventhub_name=EVENT_HUB_NAME,
        credential=credential,
    )

    with producer:
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(data)))
        producer.send_batch(event_data_batch)

    print(f"Sent message to Event Hub successfully!")

print("Starting event stream..............")
# Continuous loop
while True:
    # Generate a new uploadIdentifier for each iteration
    upload_identifier = str(uuid.uuid4())

    # Send `sql-mi.json` and `sql-vm.json` from `sku` folder
    for filename in ["sku-db.json", "sku-mi.json", "sku-vm.json"]:
        file_path = os.path.join(sku_folder_path, filename)
        file_content = read_json_file(file_path)
        if file_content:
            message = {
                "uploadIdentifier": upload_identifier,
                "body": file_content
            }
            send_to_event_hub(message)

    # Send all `.json` files from `suitability` folder
    if os.path.exists(suitability_folder_path):
        for file_name in os.listdir(suitability_folder_path):
            if file_name.endswith(".json"):
                file_path = os.path.join(suitability_folder_path, file_name)
                file_content = read_json_file(file_path)

                if file_content:
                    message = {
                        "uploadIdentifier": upload_identifier,
                        "body": file_content
                    }
                    send_to_event_hub(message)

    print("Data sent successfully! Waiting for next loop...\n")

    # Wait before the next iteration (adjust as needed)
    time.sleep(10)  # Sends data every 10 seconds
