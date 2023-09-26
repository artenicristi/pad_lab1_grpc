from sqlalchemy import create_engine, Column, String, Integer, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.orm import declarative_base

engine = create_engine('sqlite:///grpc.db')

Base = declarative_base()


class PayloadModel(Base):
    __tablename__ = 'payloads'

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String)
    topic = Column(String)
    content = Column(String)

    def __init__(self, type, topic, content):
        self.type = type
        self.topic = topic
        self.content = content

    @staticmethod
    def as_dict(type, topic, content=None):
        return {
            'type': type,
            'topic': topic,
            'content': content
        }


class ConnectionModel(Base):
    __tablename__ = 'connections'

    topic = Column(String)
    host = Column(String)
    port = Column(String)

    __table_args__ = (PrimaryKeyConstraint("topic", "host", "port", name="unique_address"),)

    def __init__(self, topic, host, port):
        self.topic = topic
        self.host = host
        self.port = port


Base.metadata.create_all(engine)
