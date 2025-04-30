from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer, KafkaConsumer
import json

class MessageModel(BaseModel):
    topic : str
    message : str


app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)



@app.post("/send")
def sendtokafka(item: MessageModel):
    producer.send(item.topic, value={"message":item.message})
    print(item)

    return item


@app.get("/receive/{topik}")
def receive(topik:str):
    # get from kafka
    consumer = KafkaConsumer(
        topik,
        bootstrap_servers='127.0.0.1:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    msg = next(consumer)
    print(msg)
    consumer.close()
    return {"ret":msg.value}
