import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

AUTORIDADES = ["CORPORINOQUIA", "CODECHOCO", "EPA Cartagena", "AMVA",
               "CRA", "CORPONOR", "SDA", "AMB", "DAGMA", "CARDER"]

VARIABLES = {"PM2.5": (5, 60), "PM10": (10, 100),
             "O3": (10, 90), "NO2": (5, 70)}


def generar_medicion():
    variable = random.choice(list(VARIABLES))
    minimo, maximo = VARIABLES[variable]
    return {
        "autoridad": random.choice(AUTORIDADES),
        "variable": variable,
        "concentracion": round(random.uniform(minimo, maximo), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

while True:
    medicion = generar_medicion()
    producer.send("calidad_aire", value=medicion)
    print(f"Sent: {medicion}")
    time.sleep(1)
