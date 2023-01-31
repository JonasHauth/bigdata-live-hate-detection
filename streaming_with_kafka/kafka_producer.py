from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import pickle
import random

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:29092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    # Nicht benötigt, da Prozesse mithalten können 
    # tweetsPerSecond = 1/10000
    # time.sleep(tweetsPerSecond)

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))


    # Dokument mit Liste von Tweet-Texts werden zufällig ausgewählt
    with open("C:/Users/jdhau/Documents/GitHub/bigdata-live-hate-detection/data/text_list", "rb") as fp:   # Unpickling
        text_list = pickle.load(fp)

    message = None

    for i in range(400000):
        i = i + 1
        message = {}

        # Debug
        # print("Preparing message: " + str(i))

        message["tweet_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message["order_id"] = i
        message["text"] = random.choice(text_list)

        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        


    print("Kafka Producer Application Completed. ")