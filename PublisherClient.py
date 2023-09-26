import grpc
import publisher_pb2 as publisher__pb2
import publisher_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')  # Replace with your server address and port.
client = publisher_pb2_grpc.PublisherStub(channel)

# Create a request
request = publisher__pb2.PublishRequest(topic='your_topic', content='your_content')

# Make a gRPC request
response = client.PublishMessage(request)

# Process the response
if response.is_success:
    print('Message published successfully')
else:
    print('Message publication failed')


