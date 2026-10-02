import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import os
import uuid
import psycopg
import altair as alt

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT",
            acks="all",
            linger_ms=5
        )

        progress_bar = st.progress(0)
        total_rows = len(df)

        for start in range(0, total_rows, 1000):
            batch = df.iloc[start:start + 1000]
            transaction_ids = (
                batch['transaction_id'].map(str)
                if 'transaction_id' in batch.columns
                else (str(uuid.uuid4()) for _ in range(len(batch)))
            )
            records = json.loads(
                batch.drop(columns='transaction_id', errors='ignore').to_json(orient='records')
            )
            futures = [
                producer.send(topic, value={"transaction_id": transaction_id, "data": record})
                for transaction_id, record in zip(transaction_ids, records)
            ]
            # Сначала отправляем всю пачку, затем проверяем доставку каждой строки.
            for future in futures:
                future.get(timeout=30)
            progress_bar.progress(min(start + len(batch), total_rows) / total_rows)

        producer.flush(timeout=30)

        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False
    finally:
        if producer is not None:
            producer.close(timeout=0)

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")

    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])

        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")

        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

st.subheader("Результаты скоринга")
if st.button("Посмотреть результаты"):
    try:
        with psycopg.connect(os.environ['DATABASE_URL']) as conn:
            frauds = pd.DataFrame(conn.execute(
                'SELECT transaction_id, score, fraud_flag FROM scores '
                'WHERE fraud_flag = 1 ORDER BY id DESC LIMIT 10'
            ).fetchall(), columns=['transaction_id', 'score', 'fraud_flag'])
            recent = pd.DataFrame(conn.execute(
                'SELECT score FROM scores ORDER BY id DESC LIMIT 100'
            ).fetchall(), columns=['score'])
        st.write("10 последних транзакций с fraud_flag = 1")
        if frauds.empty:
            st.info("Фродовых транзакций пока нет")
        else:
            st.dataframe(frauds, hide_index=True)
        st.write("Распределение скоров последних 100 транзакций")
        if recent.empty:
            st.info("Результатов пока нет. Отправьте CSV и нажмите кнопку ещё раз.")
        else:
            st.altair_chart(alt.Chart(recent).mark_bar().encode(
                x=alt.X('score:Q', bin=alt.Bin(maxbins=20), title='Скор'),
                y=alt.Y('count():Q', title='Количество транзакций'),
            ), use_container_width=True)
    except psycopg.Error as e:
        st.error(f"Ошибка PostgreSQL: {e}")
