from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import pickle
import random

# Install needed Python Libraries:
# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:29092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    # Prozess kann bei Bedarf verlangsamt werden. War für unsere Tests nicht notwendig, da das Skript selbst genug Rechenzeit benötigt.
    # tweetsPerSecond = 1/10000
    # time.sleep(tweetsPerSecond)

    # Kafka Objekt mit oben definierten Parametern erzeugen.
    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))


    # Dokument mit Liste von Tweet-Texts werden zufällig ausgewählt
    with open("C:/Users/jdhau/Documents/GitHub/bigdata-live-hate-detection/data/random_tweet_list", "rb") as fp:   # Unpickling
        text_list = pickle.load(fp)

    message = None

    # Um eine unbegrenzte Ausführung zu verhindern wird eine obere Anzahl der Tweets festgesetzt. 
    for i in range(400000):
        i = i + 1
        message = {}

        # Für Debbuging-Zwecke können die Messages ausgegeben werden. Dies ist aber nicht sinnvoll, da es die Ausführung deutlich verlangsamt.
        # print("Preparing message: " + str(i))

        message["tweet_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message["order_id"] = i
        message["text"] = random.choice(text_list)

        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        


    print("Kafka Producer Application Completed. ")