import grpc

import sender_pb2_grpc
import sender_pb2
import subscriber_pb2 as subscriber__pb2
import subscriber_pb2_grpc
from concurrent import futures


# class SubscriberService(subscriber_pb2_grpc.SubscriberServicer):
#     def Subscribe(self, request, context):
#         # Implement your logic here to handle the Subscribe request.
#         # 'request' contains the data sent by the client.
#
#         # For example, you can print the received topic, host, and port.
#         print(f"Received topic: {request.topic}, Host: {request.host}, Port: {request.port}")
#
#         # You can construct and return a response message.
#         response = subscriber__pb2.SubscribeResponse(is_success=True)
#
#         return response

class SenderService(sender_pb2_grpc.SenderServicer):
    def SendMessage(self, request, context):
        # Implement your server logic here.
        # 'request' contains the data sent by the client.

        # For example, you can print the received content.
        print(f"Received message from broker: {request}")

        # You can construct and return a response message.
        response = sender_pb2.SendResponse(is_success=True)

        return response



server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
sender_pb2_grpc.add_SenderServicer_to_server(SenderService(), server)
port = server.add_insecure_port('localhost:0')  # Bind to an available port

print(f"Subscriber listening on port {port}.")

channel = grpc.insecure_channel('localhost:50051')  # Connect to the server on the specified port
stub = subscriber_pb2_grpc.SubscriberStub(channel)

# Create a request message
request = subscriber__pb2.SubscribeRequest(topic="subscriber#", host="localhost", port=port)

# Send the request to the server
response = stub.Subscribe(request)

# Process the response from the server
if response.is_success:
    print("Subscribe successful.")
else:
    print("Subscribe failed.")

server.start()
# The server start() method is non-blocking. A new thread will be instantiated to handle requests.
# The thread calling server.start() will often not have any other work to do in the meantime.
# In this case, you can call server.wait_for_termination()
# to cleanly block the calling thread until the server terminates.
server.wait_for_termination()