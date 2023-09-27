from concurrent import futures

import grpc

import sender_pb2
import sender_pb2_grpc
import settings
import subscriber_pb2 as subscriber__pb2
import subscriber_pb2_grpc
from ConnectionRepository import ConnectionRepository


class SenderService(sender_pb2_grpc.SenderServicer):
    def SendMessage(self, request, context):
        print(f"Received content from broker:\n {request}")

        response = sender_pb2.SendResponse(is_success=True)

        return response


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
sender_pb2_grpc.add_SenderServicer_to_server(SenderService(), server)
port = server.add_insecure_port('localhost:0')

print(f"Subscriber listening on port {port}.")
host = 'localhost'

channel = grpc.insecure_channel(f"{settings.Settings.BROKER_HOST}:{settings.Settings.BROKER_PORT}")
stub = subscriber_pb2_grpc.SubscriberStub(channel)

topic = input("\nEnter the topic to subscribe: ")

request = subscriber__pb2.SubscribeRequest(topic=topic, host=host, port=port)

response = stub.Subscribe(request)

if response.is_success:
    ConnectionRepository.add(topic, "localhost", port)
    print("Subscribe successful.")

    try:
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt as e:
        ConnectionRepository.delete_by_host_port(address=(host, port))
        print(f"Subscriber was closed {e}")
    except Exception as e:
        ConnectionRepository.delete_by_host_port(address=(host, port))
        print(f"Error on subscriber: {e}")

    # The server start() method is non-blocking. A new thread will be instantiated to handle requests.
    # The thread calling server.start() will often not have any other work to do in the meantime.
    # In this case, you can call server.wait_for_termination()
    # to cleanly block the calling thread until the server terminates.

else:
    print("Subscribe failed.")
