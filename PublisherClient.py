import grpc
import publisher_pb2 as publisher__pb2
import publisher_pb2_grpc
import settings
from PayloadRepository import PayloadRepository

channel = grpc.insecure_channel(f"{settings.Settings.BROKER_HOST}:{settings.Settings.BROKER_PORT}")
client = publisher_pb2_grpc.PublisherStub(channel)

while True:
    topic = input("\nEnter topic: ").lower().strip()
    content = input("Enter content: ")
    request = publisher__pb2.PublishRequest(topic=topic, content=content)

    response = client.PublishMessage(request)

    if response.is_success:
        PayloadRepository.add(PayloadRepository.CONTENT, topic, content)
        print("Message published successfully\n")
    else:
        print("Message publication failed\n")

    input("Press enter to add new message")




