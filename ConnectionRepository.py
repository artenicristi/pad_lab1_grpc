import threading

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import GrpcDB
from GrpcDB import ConnectionModel

Session = sessionmaker(bind=GrpcDB.engine)
session = Session()

session_lock = threading.Lock()


class ConnectionRepository:

    @staticmethod
    def add(topic, host, port):
        with session_lock:
            try:
                connection = ConnectionModel(topic, host, port)
                session.add(connection)
                session.commit()
            except IntegrityError as e:
                print(f"Failed to add new connection: {e.statement}")
                session.rollback()

    @staticmethod
    def get_by_topic(topic):
        with session_lock:
            connections = session.query(ConnectionModel).filter(ConnectionModel.topic == topic)
            return connections

    @staticmethod
    def delete_by_host_port(address):
        host, port = address
        with session_lock:
            try:
                connection_to_delete = session.query(ConnectionModel).filter(
                    ConnectionModel.host == host, ConnectionModel.port == port
                ).one_or_none()

                if connection_to_delete:
                    session.delete(connection_to_delete)
                    session.commit()
                else:
                    print(f"No connection found with host {host} and port {port}. Nothing deleted.")

            except IntegrityError as e:
                print(f"Failed to delete connection: {e.statement}")
                session.rollback()
