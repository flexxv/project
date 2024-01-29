from fake_useragent import UserAgent
import requests
from bs4 import BeautifulSoup


class Parser:
    """
    Класс для получения данных со страницы
    """

    def __init__(self):
        """
        Инициализирует значения для переменных
        """
        self.headers = {
            'user_agent': UserAgent().random
        }  # заголовок запроса
        self.url = 'https://horosho-tam.ru/rossiya/coronavirus'  # ссылка на страницу
        self.infected_in_week = ''  # количество зараженных за неделю
        self.recovered_in_week = ''  # количество выздоровлений за неделю
        self.deaths_in_week = ''  # количество смертей за неделю
        self.infected_all_time = ''  # количество зараженных за всё время
        self.recovered_all_time = ''  # количество выздоровлений за всё время
        self.deaths_all_time = ''  # количество смертей за всё время

    def get_response(self):
        """
        Делает запрос на сайт и возвращает ответ
        """
        try:
            response = requests.get(self.url, headers=self.headers)
            soup = BeautifulSoup(response.text, features='html.parser')
            return soup
        except Exception as ex:
            print('Произошла ошибка при запросе на сайт')
            print(ex)

    def get_number_of_infected(self, soup):
        """
        Получает количество зараженных за неделю и за всё время
        """
        try:
            infected_all = (soup.find('tr', class_='tb_counter_even')
                            .find_all('b', class_='tb_counter_sum tb_counter_sum_cases'))

            self.infected_in_week = infected_all[0].text.replace(' ', '')
            self.infected_all_time = infected_all[2].text.replace(' ', '')
            return self.infected_in_week, self.infected_all_time
        except Exception as ex:
            print('Произошла ошибка при получении данных о зараженных')
            print(ex)

    def get_number_of_recovered(self, soup):
        """
        Получает количество выздоровлений за неделю и за всё время
        """
        try:
            recovered_all = (soup.find_all('tr', class_='tb_counter_even')[1]
                             .find_all('b', class_='tb_counter_sum tb_counter_sum_recover'))
            self.recovered_in_week = recovered_all[0].text.replace(' ', '')
            self.recovered_all_time = recovered_all[2].text.replace(' ', '')
            return self.recovered_in_week, self.recovered_all_time
        except Exception as ex:
            print('Произошла ошибка при получении данных о выздоровлений')
            print(ex)

    def get_number_of_deaths(self, soup):
        """
        Получает количество смертей за неделю и за всё время
        """
        try:
            deaths_all = (soup.find('tr', class_='tb_counter_odd')
                          .find_all('b',  class_='tb_counter_sum tb_counter_sum_deaths'))
            self.deaths_in_week = deaths_all[0].text.replace(' ', '')
            self.deaths_all_time = deaths_all[2].text.replace(' ', '')
            return self.deaths_in_week, self.deaths_all_time
        except Exception as ex:
            print('Произошла ошибка при получении данных о смертях')
            print(ex)
