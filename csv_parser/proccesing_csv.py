import csv
import pymysql
import pymysql.cursors
import configparser

DATABASE_NAME = 'proccesing_csv'  # название базы данных
TABLE_NAME = 'data_in_russia'  # название таблицы данных
FILE_NAME = r'csv_parser\data.csv'  # название файла данных


def read_config():
    """
    Чтение файла конфигурации
    """

    config = configparser.ConfigParser()  # создание объекта парсера файла конфигурации
    config.read(r"csv_parser\config.ini")  # чтение файл конфигурации
    return config


def connect_to_database(config):
    """
    Cоединение с базой данных MySQL
    """
    try:
        connection = pymysql.connect(
            host=config['DataBase']['host'],
            port=int(config['DataBase']['port']),
            user=config['DataBase']['user'],
            password=config['DataBase']['password'],
            database=DATABASE_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Соеднинение с базой данных установлено")
        print("#" * 20)
        return connection
    except Exception as ex:
        print("Не произошло соеднинения с базой данных")
        print(ex)
        print("#" * 20)


def create_table_statistics(headers_table: list, connection: pymysql.Connection):
    """
    Создает таблицу в базе данных, если она не существует

    Аргументы:
        headers_table (list): Список закголовков из csv таблицы.
        connection (pymysql.Connection): Подключение к базе данных MySQL.
    """
    try:
        with connection.cursor() as cursor:
            # запрос к базе данных, чтобы узнать существует ли таблица
            table_exists_query = cursor.execute("SELECT * FROM information_schema.tables WHERE table_schema = "
                                                f"'{DATABASE_NAME}' AND table_name = '{TABLE_NAME}' LIMIT 1;")
            # если таблица не существует, то делает запрос на её создание
            if table_exists_query == 0:
                # редактирование строки под формат запроса к таблице
                headers_table = f" varchar(32), ".join(headers_table).replace("-", "_")
                create_table_query = (
                    f"CREATE TABLE `{TABLE_NAME}`({headers_table} varchar(32));")
                cursor.execute(create_table_query)
                print("Таблица создана")
                print("#" * 20)
            else:
                print("Таблица уже существует")
                print("#" * 20)
    except Exception as exc:
        print("Ошибка при создании таблицы")
        print(exc)


def insert_data_for_russia(collected_data: list, connection: pymysql.Connection):
    """
    Добавляет данные в таблицу

    Аргументы:
    collected_data (list): Список списков построчных значений для России из csv таблицы.
    connection (pymysql.Connection): Подключение к базе данных MySQL.

    """
    try:
        with connection.cursor() as cursor:
            for data_in_row in collected_data:
                data_in_row = f"', '".join(data_in_row) # редактирование строки под формат запроса к таблице
                cursor.execute(f"INSERT INTO {DATABASE_NAME}.{TABLE_NAME} VALUES ('{data_in_row}');")
                connection.commit()  # сохранение изменений в таблице
    except Exception as exc:
        print("Ошибка при добавлении данных в таблицу")
        print(exc)


def get_data_for_russia(filename: str):
    """
    Получение данных по России из файла

    Аргументы
    filename (str): Путь к файлу с данными.
    """
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            file_riders = csv.reader(csvfile, delimiter=',')
            reader = csv.DictReader(csvfile)
            headers_name = reader.fieldnames  # заголовки таблицы
            # генерация списка данных по России из csv файла
            values_for_russia = [row for row in file_riders if 'Russia' in row]
            return values_for_russia, headers_name
    except Exception as exc:
        print("Ошибка при получении данных из файла")
        print(exc)


def processing_empty_value(value: str):
    """
    Обрабатывает пустые значения и заменяет их на "0"

    Аргументы:
    value (str): Значение для обработки.
    """
    try:
        if value == '':
            return '0'
        else:
            return value
    except Exception as exc:
        print("Ошибка при обработке пустых значений")
        print(exc)


# собирает обработанные данные
def collecting_corrected_data(values: list):
    """
    Собирает данные и обрабатывает пустые значения

    Аргументы:
    values (list): Список собранных значений
    """
    try:
        # применение функции обработки пустых значений и генерация списка
        collected_data = [list(map(processing_empty_value, row)) for row in values]
        return collected_data
    except Exception as exc:
        print("Ошибка при обработке данных")
        print(exc)


def main():
    print('Чтение файла конфигурации')
    print("#" * 20)
    config = read_config()
    print('Соединение с базой данных')
    print("#" * 20)
    connection = connect_to_database(config)
    print('Получение данных по России из csv файла')
    print("#" * 20)
    values_for_russia, headers_name = get_data_for_russia(FILE_NAME)
    print('Сбор обработанных данных')
    print("#" * 20)
    collected_data = collecting_corrected_data(values_for_russia)
    print('Создаение таблицы данных')
    print("#" * 20)
    create_table_statistics(headers_name, connection)
    print('Добавление данных в таблицу')
    print("#" * 20)
    insert_data_for_russia(collected_data, connection)


if __name__ == '__main__':
    main()
