import json
import logging

from tools.pg_tools import insert_data_into_pg, run_query

from config import DB_PG_CONN

TABLE_NAME = "nhl"

SCHEMA = "raw"


def _transform_data_nhl(data):

    """
    Преобразование полученных данных
    """

    logging.info("Преобразование значений для записи")

    count_rows = len(data)

    if isinstance(data, list) and count_rows != 0:
        for i, row in enumerate(data):
            for key, value in row.items():
                row[key] = json.dumps(value)
    else:
        raise Exception("Данные для вставки отсутствуют")

    logging.info("Данные преобразованы")

    return data, count_rows


def test_count_row(count_rows):

    logging.info("Производим тест на количество строк")

    sql_count_row = f"""
    SELECT COUNT(*) FROM {SCHEMA}.{TABLE_NAME}
    """

    count_row_in_db = run_query(DB_PG_CONN, sql_count_row, data=True)

    if count_row_in_db == count_rows:
        return logging.info("Кол-во строк совпадает")
    else:
        raise Exception("Кол-во строк НЕ совпадает")


def load_data_nhl(**context):

    """
    Очистка таблицы и загрузка данных в таблицу. Также производится тест на кол-во строк.
    """

    # Получаем путь и имя файла из xcomm
    path_and_file_name = context['task_instance'].xcom_pull(task_ids='import_nhl_data')

    # Читаем файл
    with open(path_and_file_name, "r", encoding="utf-8") as f:
        data = json.loads(f.read())['stats']

    pure_data, count_rows = _transform_data_nhl(data)

    # Очищаем таблицу
    run_query(DB_PG_CONN, f"TRUNCATE {TABLE_NAME}")
    logging.info("Таблица для записи очищена")

    # Вставляем файл
    insert_data_into_pg(data=pure_data,
                        connection=DB_PG_CONN,
                        schema=SCHEMA,
                        table_name=TABLE_NAME)

    logging.info("Данные успешно загружены")

    # Тест на кол-во строк
    test_count_row(count_rows)
