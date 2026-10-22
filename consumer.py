import json
import sqlite3
from kafka import KafkaConsumer

TOPICO = "transacoes"

# Banco de dados: cria o arquivo fraude.db e a tabela, se ainda não existirem
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
    fraudes = []          # as regras entram na próxima etapa
    salvar(t, fraudes)
    print(f"✅ Transação salva -> {t}")