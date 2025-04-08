from azure.eventhub import EventHubConsumerClient
from azure.identity import DefaultAzureCredential
import json

# Replace with your values
FULLY_QUALIFIED_NAMESPACE = "pricing-streaming.servicebus.windows.net"
EVENT_HUB_NAME = "streaming-output"
CONSUMER_GROUP = "$Default"

# Use Azure AD-based credential
credential = DefaultAzureCredential()

def on_event(partition_context, event):
    try:
        # Read body as string
        body_str = event.body_as_str(encoding='utf-8')
        # Try to parse as JSON
        body_json = json.loads(body_str)
        print(f"\n✅ JSON Message from partition {partition_context.partition_id}:\n{json.dumps(body_json, indent=2)}")
    except json.JSONDecodeError:
        print(f"\n⚠️ Non-JSON message: {body_str}")
    except Exception as e:
        print(f"🔥 Error reading event: {e}")

    # Mark the event as processed
    partition_context.update_checkpoint(event)

# Create client using AAD credential
client = EventHubConsumerClient(
    fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
    eventhub_name=EVENT_HUB_NAME,
    consumer_group=CONSUMER_GROUP,
    credential=credential,
)

print("🚀 Listening for messages...")
with client:
    client.receive(
        on_event=on_event,
        starting_position="-1",
    )
