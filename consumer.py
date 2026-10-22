import json
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timedelta
from kafka import KafkaConsumer

TOPICO = "transacoes"

# Banco de dados
conn = sqlite3.connect("fraude.db")
conn.execute("""
    CREATE TABLE IF NOT EXISTS transacoes (
        transaction_id TEXT PRIMARY KEY,
        customer_id    TEXT,
        amount         REAL,
        city           TEXT,
        timestamp      TEXT,
        fraude         INTEGER,
        tipos_fraude   TEXT
    )
""")
conn.commit()

# Histórico em memória: últimas transações de cada cliente
historico = defaultdict(lambda: deque(maxlen=50))


def detectar_fraudes(t, ts, hist):
    fraudes = []

    # Regra 1: Alto Valor
    if t["amount"] >= 10000:
        fraudes.append("ALTO_VALOR")

    # Regra 2: Tempo (esta + 3 anteriores em menos de 60s = 4 transações)
    ultimos_60s = [h for h in hist if ts - h["ts"] < timedelta(seconds=60)]
    if len(ultimos_60s) >= 3:
        fraudes.append("TEMPO_60s")

    # Regra 3: GEO (cidade diferente em até 10 minutos)
    for h in hist:
        if ts - h["ts"] <= timedelta(minutes=10) and h["city"] != t["city"]:
            fraudes.append("GEO_10m")
            break

    return fraudes


def salvar(t, fraudes):
    conn.execute(
        "INSERT OR IGNORE INTO transacoes VALUES (?,?,?,?,?,?,?)",
        (
            t["transaction_id"], t["customer_id"], t["amount"],
            t["city"], t["timestamp"],
            1 if fraudes else 0, ",".join(fraudes),
        ),
    )
    conn.commit()


consumer = KafkaConsumer(
    TOPICO,
    bootstrap_servers="localhost:9092",
    group_id="detector-fraude",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("Consumer aguardando transações...")

for msg in consumer:
    t = msg.value
    ts = datetime.fromisoformat(t["timestamp"])
    hist = historico[t["customer_id"]]

    fraudes = detectar_fraudes(t, ts, hist)
    salvar(t, fraudes)
    hist.append({"ts": ts, "city": t["city"]})

    if fraudes:
        print(f"🚨 FRAUDE {fraudes} -> {t}")
    else:
        print(f"✅ Transação normal -> {t}")