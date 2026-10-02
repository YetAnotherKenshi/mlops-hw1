import os
import sys
import pandas as pd
import logging
import json

from confluent_kafka import Consumer, Producer

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from preprocessing import load_train_data, run_preproc
from scorer import make_pred

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scores")


def check_delivery(error, message):
    if error is not None:
        raise RuntimeError(f"Kafka delivery failed: {error}")


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.producer_config = {
             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
             }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)

        # Загрузка данных для препроцессинга
        self.train = load_train_data()

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                # Десериализация JSON
                data = json.loads(msg.value().decode('utf-8'))

                # Извлекаем ID и данные
                transaction_id = data['transaction_id']
                input_df = pd.DataFrame([data['data']])

                # Препроцессинг и предсказание
                processed_df = run_preproc(self.train, input_df)
                submission = make_pred(processed_df, "kafka_stream")

                # Добавляем ID в результат
                submission['transaction_id'] = transaction_id

                # Один JSON-объект с тремя полями на транзакцию
                self.producer.produce(
                    SCORING_TOPIC,
                    key=str(transaction_id),
                    value=submission.iloc[0].to_json(),
                    callback=check_delivery
                )
                if self.producer.flush(30):
                    raise TimeoutError("Kafka delivery timed out")
                self.consumer.commit(message=msg, asynchronous=False)
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                raise


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
    finally:
        service.consumer.close()
