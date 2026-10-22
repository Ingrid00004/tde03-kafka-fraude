# TDE 03 - Detecção de Fraude com Apache Kafka

Projeto individual que simula um sistema de detecção de fraude em transações usando **Apache Kafka** e **Python**.

## Como funciona

- O **Producer** gera uma transação aleatória a cada 3 segundos e publica no tópico `transacoes` do Kafka.
- O **Consumer** lê as transações, aplica as regras de detecção de fraude, salva todas no banco de dados SQLite e exibe no console se a transação é normal ou fraude.

```
producer.py  →  Kafka (Docker)  →  consumer.py  →  fraude.db (SQLite)
                                        ↓
                                 alertas no console
```

## Tecnologias

- Python 3
- Apache Kafka (executado com Docker Compose)
- SQLite
- Biblioteca `kafka-python`

## Padrão da transação

```json
{
  "transaction_id": "7dca0d1b-e83f-49fd-b95d-6d0fc8d1063f",
  "customer_id": "C005",
  "amount": 93.19,
  "city": "Rio de Janeiro",
  "timestamp": "2026-10-21T22:17:20.377521"
}
```

| Campo | Descrição |
|---|---|
| `transaction_id` | Identificador único da transação (UUID) |
| `customer_id` | Identificador do cliente |
| `amount` | Valor da transação |
| `city` | Cidade onde a transação ocorreu |
| `timestamp` | Data e hora da transação |

## Regras de fraude

| Regra | Condição |
|---|---|
| `ALTO_VALOR` | Transação com `amount >= 10000` |
| `TEMPO_60s` | Mesmo cliente com 4 transações em menos de 60 segundos |
| `GEO_10m` | Mesmo cliente com transações em cidades diferentes em um intervalo de 10 minutos |

Uma mesma transação pode ter mais de um tipo de fraude (por exemplo, `ALTO_VALOR,GEO_10m`).

## Banco de dados

Todas as transações (normais e fraudulentas) são salvas no arquivo `fraude.db`, na tabela `transacoes`:

| Coluna | Descrição |
|---|---|
| `transaction_id` | Identificador da transação (chave primária) |
| `customer_id` | Cliente |
| `amount` | Valor |
| `city` | Cidade |
| `timestamp` | Data e hora |
| `fraude` | `1` se for fraude, `0` se for normal |
| `tipos_fraude` | Tipos de fraude detectados, separados por vírgula |

## Arquivos

| Arquivo | Função |
|---|---|
| `docker-compose.yml` | Sobe o Kafka |
| `producer.py` | Gera e envia as transações |
| `consumer.py` | Lê as transações, aplica as regras, salva no banco e exibe no console |
| `requirements.txt` | Dependências do Python |

## Como executar

Pré-requisitos: Python 3 e Docker Desktop instalados e em execução.

**1. Subir o Kafka**

```
docker compose up -d
```

Aguarde cerca de 20 segundos para o Kafka iniciar.

**2. Criar e ativar o ambiente virtual**

```
python -m venv venv
venv\Scripts\activate
```

**3. Instalar as dependências**

```
pip install -r requirements.txt
```

**4. Iniciar o consumer (terminal 1)**

```
python consumer.py
```

**5. Iniciar o producer (terminal 2)**

```
python producer.py
```

**6. Consultar as fraudes salvas no banco**

Com o consumer parado:

```
python -c "import sqlite3; [print(l) for l in sqlite3.connect('fraude.db').execute('SELECT * FROM transacoes WHERE fraude = 1')]"
```

Para parar, use `Ctrl + C` em cada terminal. Para desligar o Kafka:

```
docker compose down
```