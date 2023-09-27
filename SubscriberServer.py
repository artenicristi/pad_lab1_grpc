import atexit
import signal
import threading
import time

import grpc

import sender_pb2_grpc
import sender_pb2
import settings
import subscriber_pb2 as subscriber__pb2
import subscriber_pb2_grpc
from concurrent import futures

from ConnectionRepository import ConnectionRepository


def send_notification():
    channel = grpc.insecure_channel(f"{settings.Settings.BROKER_HOST}:{settings.Settings.BROKER_PORT}")
    stub = sender_pb2_grpc.SenderStub(channel)

    # Create a request message
    request = sender_pb2.SendRequest(content=f"Delete connection")

    # Send the request to the server
    response = stub.SendMessage(request)

    if response.is_success:
        print("Message successfully delivered.")
    else:
        print("Failed to deliver message.")


class SenderService(sender_pb2_grpc.SenderServicer):
    def SendMessage(self, request, context):
        print(f"Received content from broker:\n {request}")

        response = sender_pb2.SendResponse(is_success=True)

        return response


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
sender_pb2_grpc.add_SenderServicer_to_server(SenderService(), server)
port = server.add_insecure_port('localhost:0')

print(f"Subscriber listening on port {port}.")

channel = grpc.insecure_channel(f"{settings.Settings.BROKER_HOST}:{settings.Settings.BROKER_PORT}")
stub = subscriber_pb2_grpc.SubscriberStub(channel)

topic = input("\nEnter the topic to subscribe: ")

request = subscriber__pb2.SubscribeRequest(topic=topic, host="localhost", port=port)

response = stub.Subscribe(request)

if response.is_success:
    ConnectionRepository.add(topic, "localhost", port)
    print("Subscribe successful.")

    try:
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        send_notification()
    except Exception as e:
        print(f"Subscriber closed: {e}")

    # signal.signal(signal.SIGINT, send_notification)
    # atexit.register(send_notification)

    # Now, the server has terminated. Start a new thread to send the notification.
    # notification_thread = threading.Thread(target=send_notification)
    # notification_thread.start()
    # notification_thread.join()

    # The server start() method is non-blocking. A new thread will be instantiated to handle requests.
    # The thread calling server.start() will often not have any other work to do in the meantime.
    # In this case, you can call server.wait_for_termination()
    # to cleanly block the calling thread until the server terminates.

    # server.wait_for_termination()
else:
    print("Subscribe failed.")
