import threading
from concurrent import futures

import grpc

import publisher_pb2 as publisher__pb2
import publisher_pb2_grpc
import sender_pb2
import sender_pb2_grpc
import subscriber_pb2 as subscriber__pb2
import subscriber_pb2_grpc


def send_message(topic, host, port):
    print(f"Request from Subscriber:\n {topic} ++ {host} ++ {port}")
    channel = grpc.insecure_channel(f"{host}:{port}")  # Connect to the server on the specified port
    stub = sender_pb2_grpc.SenderStub(channel)

    # Create a request message
    request = sender_pb2.SendRequest(content=f"Hello, Subscriber:{port} from Broker!")

    # Send the request to the server
    response = stub.SendMessage(request)

    # Process the response from the server
    if response.is_success:
        print("Message successfully delivered.")
    else:
        print("Failed to deliver message.")

    return response


class PublisherServicer(publisher_pb2_grpc.PublisherServicer):
    def PublishMessage(self, request, context):
        # Implement your logic to process the request here.
        # You can access the request parameters like request.topic and request.content.
        # You should also create a response using publisher_pb2.PublishResponse.
        print(f"Received from Publisher: {request}")

        # Example response:
        response = publisher__pb2.PublishResponse(is_success=True)
        return response


class SubscriberService(subscriber_pb2_grpc.SubscriberServicer):
    def Subscribe(self, request, context):
        # Implement your logic here to handle the Subscribe request.
        # 'request' contains the data sent by the client.

        # For example, you can print the received topic, host, and port.
        print(f"Received Subscribe: {request}")

        # asta trebuie sa fac in worker
        subscriber_thread = threading.Thread(target=send_message, args=(request.topic, request.host, request.port))
        subscriber_thread.start()

        # You can construct and return a response message.
        response = subscriber__pb2.SubscribeResponse(is_success=True)
        return response



server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
subscriber_pb2_grpc.add_SubscriberServicer_to_server(SubscriberService(), server)
publisher_pb2_grpc.add_PublisherServicer_to_server(PublisherServicer(), server)

print('Starting server. Listening on port 50051.')
# server.add_insecure_port('[::]:50051')  # Replace with your desired server address and port.
# de vazut cum de facut bind sau ce hosturi accepta
server.add_insecure_port('localhost:50051')  # Replace with your desired server address and port.
server.start()
server.wait_for_termination()
