import logging
import psycopg2

from psycopg2.extras import execute_batch


def run_query(connection_string, query, data=None):

    conn_db = psycopg2.connect(connection_string)

    cursor = conn_db.cursor()

    if data:
        cursor.execute(query, data)
    else:
        cursor.execute(query)

    try:
        result = cursor.fetchall()
        return result
    except psycopg2.ProgrammingError as e:
        if 'no results to fetch' in e:
            pass
        else:
            raise
    finally:
        conn_db.commit()
        cursor.close()


def insert_data_into_pg(data, connection, schema, table_name):

    """
    Функция для вставки данных в PostgreSQL
    """

    main_columns = list(data[0].keys())

    values = ['%({0})s'.format(column) for column in main_columns]
    values = ', '.join(values)

    main_columns = ['"{0}"'.format(column) for column in main_columns]
    columns = ', '.join(main_columns)

    conn_db = psycopg2.connect(connection)

    query = f"""
        INSERT INTO {schema}.{table_name} ({columns})
        VALUES ({values})
        """

    cursor = conn_db.cursor()

    execute_batch(cursor, query, data, page_size=50000)

    conn_db.commit()
    cursor.close()

    logging.info('Вставлено', str(len(data)), 'строк')
