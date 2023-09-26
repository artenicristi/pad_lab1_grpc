from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import GrpcDB
from GrpcDB import PayloadModel

Session = sessionmaker(bind=GrpcDB.engine)
session = Session()


class PayloadRepository:
    SUBSCRIBE = "subscribe@"
    CONTENT = "content"
    BATCH_SIZE = 5

    @staticmethod
    def add(type, topic, content):
        try:
            payload = PayloadModel(type, topic, content)
            session.add(payload)
            session.commit()
        except IntegrityError as e:
            print(f"Failed to add new payload: {e}")
            session.rollback()

    @staticmethod
    def delete(payload):
        try:
            session.delete(payload)
            session.commit()
        except IntegrityError as e:
            print(f"Failed to delete payload: {e}")
            session.rollback()

    @staticmethod
    def load():
        payloads = session.query(PayloadModel).limit(PayloadRepository.BATCH_SIZE)
        return payloads
