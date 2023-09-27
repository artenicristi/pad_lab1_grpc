import threading
from concurrent import futures

import grpc

import publisher_pb2 as publisher__pb2
import publisher_pb2_grpc
import sender_pb2
import sender_pb2_grpc
import settings
import subscriber_pb2 as subscriber__pb2
import subscriber_pb2_grpc


def send_message(topic, host, port):
    # asta e lucrul pentru worker
    # scot payloads, in baza la payload.topic scot connections si deja
    # fac SendRequest(content=)
    # fara topic

    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = sender_pb2_grpc.SenderStub(channel)

    # Create a request message
    request = sender_pb2.SendRequest(content=f"Content based on topic")

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
        print(f"Received from Publisher:\n {request}")

        response = publisher__pb2.PublishResponse(is_success=True)
        return response


class SubscriberService(subscriber_pb2_grpc.SubscriberServicer):
    def Subscribe(self, request, context):
        print(f"Received from Subscriber:\n {request}")

        subscriber_thread = threading.Thread(target=send_message, args=(request.topic, request.host, request.port))
        subscriber_thread.start()

        response = subscriber__pb2.SubscribeResponse(is_success=True)
        return response



class SenderService(sender_pb2_grpc.SenderServicer):
    def SendMessage(self, request, context):
        print(f"Received notification from Subscriber:\n {request}")

        response = sender_pb2.SendResponse(is_success=True)

        return response


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
subscriber_pb2_grpc.add_SubscriberServicer_to_server(SubscriberService(), server)
publisher_pb2_grpc.add_PublisherServicer_to_server(PublisherServicer(), server)
sender_pb2_grpc.add_SenderServicer_to_server(SenderService(), server)

print(f"Starting server. Listening on port {settings.Settings.BROKER_HOST}:{settings.Settings.BROKER_PORT}.")

server.add_insecure_port(f"{settings.Settings.BROKER_HOST}:{settings.Settings.BROKER_PORT}")
server.start()
server.wait_for_termination()
