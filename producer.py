import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

TOPICO = "transacoes"
CLIENTES = ["C001", "C002", "C003", "C004", "C005"]
CIDADES = ["São Paulo", "Rio de Janeiro", "Fortaleza", "Recife", "Curitiba"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def gerar_transacao():
    # 10% de chance de valor alto, para o caso ALTO_VALOR aparecer
    if random.random() < 0.10:
        amount = round(random.uniform(10000, 20000), 2)
    else:
        amount = round(random.uniform(10, 5000), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.choice(CLIENTES),
        "amount": amount,
        "city": random.choice(CIDADES),
        "timestamp": datetime.now().isoformat(),
    }

while True:
    t = gerar_transacao()
    producer.send(TOPICO, t)
    producer.flush()
    print(f"[PRODUCER] Enviada: {t}")
    time.sleep(3)