import threading
import time
from concurrent import futures

import grpc

import publisher_pb2 as publisher__pb2
import publisher_pb2_grpc
import sender_pb2
import sender_pb2_grpc
import settings
import subscriber_pb2 as subscriber__pb2
import subscriber_pb2_grpc
from ConnectionRepository import ConnectionRepository
from PayloadRepository import PayloadRepository


def worker_messages():
    while True:
        time.sleep(5)
        print("Worker messages started:\n")

        payloads = PayloadRepository.load()

        for payload in payloads:
            topic = payload.topic
            connections = ConnectionRepository.get_by_topic(topic=topic)

            if not connections.count():
                continue

            for conn in connections:
                subscriber_sender = threading.Thread(target=send_message, args=(payload.content, conn.host, conn.port))
                subscriber_sender.start()

            PayloadRepository.delete(payload)


def send_message(host, port, content):
    time.sleep(2)
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = sender_pb2_grpc.SenderStub(channel)

    request = sender_pb2.SendRequest(message="CONNECTED")

    response = stub.SendMessage(request)

    if response.is_success:
        print(f"Message successfully delivered to subscriber ({host}:{port}).")
    else:
        print(f"Failed to deliver message to subscriber ({host}:{port}).")

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


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
subscriber_pb2_grpc.add_SubscriberServicer_to_server(SubscriberService(), server)
publisher_pb2_grpc.add_PublisherServicer_to_server(PublisherServicer(), server)

print(f"Starting server. Listening on port {settings.Settings.BROKER_HOST}:{settings.Settings.BROKER_PORT}.")

# worker_thread = threading.Thread(target=worker_messages)
# worker_thread.start()

server.add_insecure_port(f"{settings.Settings.BROKER_HOST}:{settings.Settings.BROKER_PORT}")
server.start()
server.wait_for_termination()

# worker_thread.join()