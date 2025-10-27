import os
import requests
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import udf

BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PUNTAJE_URL = os.getenv("PUNTAJE_URL", "http://puntaje:5003/compute")
THRESHOLD = float(os.getenv("THRESHOLD", "0.20"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "2"))

@udf(
    result_type=DataTypes.FLOAT(),
    input_types=[DataTypes.STRING(), DataTypes.STRING()]
)
def HTTP_SCORE(human_answer: str, llm_answer: str) -> float:
    try:
        r = requests.post(
            PUNTAJE_URL,
            json={"human_answer": human_answer or "", "llm_answer": llm_answer or ""},
            timeout=15
        )
        r.raise_for_status()
        return float(r.json().get("puntaje", 0.0))
    except Exception:
        # En error devuelve 0.0 para permitir reintentos/derivación
        return 0.0

def main():
    # Modo streaming
    settings = EnvironmentSettings.in_streaming_mode()
    t = TableEnvironment.create(settings)

    # Asegura el intérprete de Python dentro de JM/TM
    t.get_config().set("python.client.executable", "/usr/bin/python3")
    t.get_config().set("python.executable", "/usr/bin/python3")

    # (Opcional) nombre de pipeline para que se vea amigable en el dashboard
    t.get_config().set("pipeline.name", "SD-Tarea2-Scoring-Validator")

    # Registra la UDF
    t.create_temporary_system_function("HTTP_SCORE", HTTP_SCORE)

    # Source: answers.success (Kafka JSON)
    t.execute_sql(f"""
    CREATE TABLE answers_success (
      qid STRING,
      question STRING,
      human_answer STRING,
      llm_answer STRING,
      attempts INT,
      `ts` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'answers.success',
      'properties.bootstrap.servers' = '{BOOT}',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'json',
      'value.json.ignore-parse-errors' = 'true'
    )
    """)

    # Sink: results.validated
    t.execute_sql(f"""
    CREATE TABLE results_validated (
      qid STRING,
      question STRING,
      human_answer STRING,
      llm_answer STRING,
      attempts INT,
      score FLOAT
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'results.validated',
      'properties.bootstrap.servers' = '{BOOT}',
      'value.format' = 'json',
      'sink.delivery-guarantee' = 'at-least-once'
    )
    """)

    # Sink: questions.pending (reintentos)
    t.execute_sql(f"""
    CREATE TABLE questions_pending (
      qid STRING,
      question STRING,
      human_answer STRING,
      attempts INT,
      retry_count INT
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'questions.pending',
      'properties.bootstrap.servers' = '{BOOT}',
      'value.format' = 'json',
      'sink.delivery-guarantee' = 'at-least-once'
    )
    """)

    # Sink: results.deadletter
    t.execute_sql(f"""
    CREATE TABLE results_deadletter (
      qid STRING,
      question STRING,
      human_answer STRING,
      llm_answer STRING,
      attempts INT,
      reason STRING
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'results.deadletter',
      'properties.bootstrap.servers' = '{BOOT}',
      'value.format' = 'json',
      'sink.delivery-guarantee' = 'at-least-once'
    )
    """)

    # Vista: computa el score una sola vez por fila
    t.execute_sql("""
    CREATE TEMPORARY VIEW scored AS
    SELECT
      qid,
      question,
      human_answer,
      llm_answer,
      attempts,
      HTTP_SCORE(human_answer, llm_answer) AS score
    FROM answers_success
    """)

    # StatementSet para multiplexar salidas
    ss = t.create_statement_set()

    # 1) Validados
    ss.add_insert_sql(f"""
      INSERT INTO results_validated
      SELECT qid, question, human_answer, llm_answer, attempts, score
      FROM scored
      WHERE score >= {THRESHOLD}
    """)

    # 2) Reintentos
    ss.add_insert_sql(f"""
      INSERT INTO questions_pending
      SELECT qid, question, human_answer, attempts + 1 AS attempts, 0 AS retry_count
      FROM scored
      WHERE score < {THRESHOLD} AND attempts < {MAX_ATTEMPTS}
    """)

    # 3) Dead Letter (máx. intentos)
    ss.add_insert_sql(f"""
      INSERT INTO results_deadletter
      SELECT qid, question, human_answer, llm_answer, attempts, 'max_attempts' AS reason
      FROM scored
      WHERE score < {THRESHOLD} AND attempts >= {MAX_ATTEMPTS}
    """)

    # Ejecuta el job en modo streaming (bloquea)
    ss.execute()

if __name__ == "__main__":
    main()