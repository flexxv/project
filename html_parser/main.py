import pars
import database
import datetime

COUNTRY = 'Russia'

DATE = (datetime.date.today())  # переменная для сегодняшней даты


def main():
    """
    Происходит инициализация необходимых объектов, получение статистики со страницы,
    создание таблицы и добавление в неё данных
    """
    p = pars.Parser()
    d = database.Database()
    #return
    print('Делаю запрос на сайт')
    print("#" * 20)
    soup = p.get_response()
    print('Получаю данные по количеству зараженных')
    print("#" * 20)
    p.infected_in_week, p.infected_all_time = p.get_number_of_infected(soup)
    print('Получаю данные по количеству смертей')
    print("#" * 20)
    p.deaths_in_week, p.deaths_all_time = p.get_number_of_deaths(soup)
    print('Получаю данные по количеству выздоровлений')
    print("#" * 20)
    p.recovered_in_week, p.recovered_all_time = p.get_number_of_recovered(soup)
    print('Создаю таблицу, если её не существует')
    print("#" * 20)
    d.create_table_statistics()
    print('Добавляю данные в таблицу')
    print("#" * 20)
    d.insert_data_statistics(date=DATE, country=COUNTRY, infection_in_week=p.infected_in_week,
                             deaths_in_week=p.deaths_in_week, recovered_in_week=p.recovered_in_week,
                             infection_all_time=p.infected_all_time, deaths_all_time=p.deaths_all_time,
                             recovered_all_time=p.recovered_all_time)
    print("Работа завершена")
    print('Для выхода нажмите любую кнопку')
    input()
    print('---------------------------\n')


if __name__ == '__main__':
    main()
