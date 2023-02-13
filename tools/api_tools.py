import json
import logging
import requests


def get_data_via_api(url: str, headers=None, params=None):

    """
    Функция получения данных через API
    """

    response = requests.get(url, headers=headers, params=params)
    result = json.loads(response.text)

    if response.status_code == 200:
        return result
    else:
        raise Exception(f"Ошибка при выгрузке данных из API: {response.text}")


def import_data_to_json(url: str, path: str, file_name: str) -> str:

    """
    Импорт данных с помощью API.
    Возвращает путь к файлу с полученными данными
    """

    result_data = get_data_via_api(url)

    logging.info("Данные получены")

    path_and_file_name = f"{path}/{file_name}.json"

    with open(path_and_file_name, "w", encoding="utf-8") as f:
        f.write(json.dumps(result_data))

    logging.info("Данные записаны в файл")

    return path_and_file_name
