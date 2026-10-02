import json
import logging
import os

import psycopg
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO)


def main():
    consumer = Consumer({
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        'group.id': 'postgres-sink',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([os.getenv('KAFKA_SCORING_TOPIC', 'scores')])
    try:
        with psycopg.connect(os.environ['DATABASE_URL']) as conn:
            while True:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    logging.error('Kafka: %s', message.error())
                    continue
                result = json.loads(message.value())
                with conn.transaction():
                    conn.execute(
                        'INSERT INTO scores (transaction_id, score, fraud_flag) '
                        'VALUES (%s, %s, %s) ON CONFLICT (transaction_id) DO NOTHING',
                        (result['transaction_id'], result['score'], result['fraud_flag']),
                    )
                consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
