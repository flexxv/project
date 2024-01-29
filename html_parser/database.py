import pymysql
import pymysql.cursors
import configparser

config = configparser.ConfigParser()  # создание объекта парсера файла конфигурации
config.read(r"html_parser\config.ini")  # чтение файл конфигурации


class Database:
    """
    Класс для взаимодействия с базой данных MySQL
    """

    def __init__(self):
        """
        Инициализирует значения для переменных и совершает соединение с базой данных
        """
        self.database_name = 'coronavirus_statistics'  # название базы данных
        self.table_name = 'statistics'  # название таблицы данных
        # соединение с базой данных
        
        
        try:
            self.connection = pymysql.connect(
                host=config['DataBase']['host'],
                port=int(config['DataBase']['port']),
                user=config['DataBase']['user'],
                password=config['DataBase']['password'],
                database=self.database_name,
                cursorclass=pymysql.cursors.DictCursor
            )
            print("Соеднинение с базой данных установлено")
            print("#" * 20)
        except Exception as ex:
            #print("Не произошло соеднинения с базой данных")
            print(ex)
            print("#" * 20)

    def create_table_statistics(self):
        """
        Создает таблицу в базе данных, если она не существует
        """
        try:
            with self.connection.cursor() as cursor:
                # запрос к базе данных, чтобы узнать существует ли таблица
                table_exists_query = cursor.execute("SELECT * FROM information_schema.tables WHERE table_schema = "
                                                    f"'{self.database_name}' AND table_name = '{self.table_name}' LIMIT 1;")
                # если таблица не существует, то делает запрос на её создание
                if table_exists_query == 0:
                    create_table_query = (
                        f"CREATE TABLE `{self.table_name}`(id int AUTO_INCREMENT, date date, country varchar(32), "
                        "infection_in_week int, deaths_in_week int,recovered_in_week int, "
                        "infection_all_time int, deaths_all_time int, recovered_all_time int, "
                        "PRIMARY KEY (id));")
                    cursor.execute(create_table_query)
                    print("Таблица создана")
                    print("#" * 20)
                else:
                    print("Таблица уже существует")
                    print("#" * 20)
        except Exception as exc:
            print("Ошибка при создании таблицы")
            print(exc)

    def insert_data_statistics(self, date, country, infection_in_week, deaths_in_week, recovered_in_week,
                               infection_all_time, deaths_all_time, recovered_all_time):
        """
        Добавляет данные в таблицу
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("INSERT INTO coronavirus_statistics.statistics(date, country, infection_in_week, "
                               "deaths_in_week,recovered_in_week, infection_all_time, deaths_all_time, recovered_all_time) "
                               f"VALUES ('{date}', '{country}', '{infection_in_week}', "
                               f"'{deaths_in_week}', '{recovered_in_week}', '{infection_all_time}', "
                               f"'{deaths_all_time}', '{recovered_all_time}');")
                self.connection.commit()  # сохранение изменений в таблице
        except Exception as exc:
            print("Ошибка при добавлении данных в таблицу")
            print(exc)
