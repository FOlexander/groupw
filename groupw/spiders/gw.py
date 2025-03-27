import scrapy
import psycopg2
from scrapy.exceptions import CloseSpider
import re
import os
import datetime
from dotenv import load_dotenv

load_dotenv()

class GwSpider(scrapy.Spider):
    name = "gw"
    allowed_domains = ["www.group-working.com"]
    start_urls = ["https://www.group-working.com/ua/jobs"]
    base_url = "https://www.group-working.com/ua/job/"

    def __init__(self):
        # Подключение к базе данных
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
        )
        self.cursor = self.conn.cursor()
        self.existing_vacancies = self.get_existing_vacancies()

    def get_existing_vacancies(self):
        """Получает список всех существующих vac_id в базе."""
        self.cursor.execute("SELECT vac_id FROM vac_form_vacancy WHERE site = %s", ('group-working.com',))
        return {row[0] for row in self.cursor.fetchall()}

    def parse(self, response):
        text = response.text
        html_content = text.replace("“", '"').replace("”", '"').replace("\\", "")
        pattern = r'"databaseId":\d+'
        links = re.findall(pattern, html_content)
        current_vacancies = set()
        # link ='https://www.group-working.com/ua/job/2975'
        # yield scrapy.Request(url=link, callback=self.parse_vacancy)


        for link in links:
            current_vacancies.add(link)

            if link not in self.existing_vacancies:
                yield scrapy.Request(url=f'{self.base_url}{link.split(":")[-1]}', callback=self.parse_vacancy)

        # Обновляем вакансии, которых больше нет на сайте
        inactive_vacancies = self.existing_vacancies - current_vacancies
        if inactive_vacancies:
            print(f"Видалені вакансії: {inactive_vacancies}")
            # Обновляем связанные записи, устанавливая selected_vacancy_id в NULL
            self.cursor.executemany(
                """
                UPDATE vac_form_jobapplication
                SET selected_vacancy_id = NULL
                WHERE selected_vacancy_id = %s
                """,
                [(vac_id,) for vac_id in inactive_vacancies]
            )
            self.conn.commit()
            # Удаляем неактивные вакансии
            self.cursor.executemany(
                "DELETE FROM vac_form_vacancy WHERE site = %s AND vac_id = %s", 
                [('group-working.com', vac_id) for vac_id in inactive_vacancies]
            )
            self.conn.commit()

    def parse_vacancy(self, response):
        print(response.url)
        # with open("test.html", "w", encoding='utf-8') as f:
        #     f.write(response.text)
        vac_id = response.url.split("/")[-1]
        position = response.css('h1::text').get().split(" – ")[0]
        job_category = response.css('.ico__profession + p::text').get()
        country = response.css('.ico__location + p::text').get().split(", ")[0]
        
        salary_list = response.css('.ico__salary + p::text').getall()
        salary = ('').join(salary_list).replace(" ", "")

        date_posted = str(datetime.datetime.now().strftime('%d.%m.%Y'))
        try:
            vaccity = response.css('.ico__location + p::text').get().split(", ")[1]
        except:
            vaccity = ''
        tools = ''
        def extract_info(pattern, text, default=''):
            match = re.search(pattern, text)
            return match.group(1) if match else default

        description_list = response.css('.open__content div > p::text').getall()
        description = ''.join(description_list)
        patterns = {
            "docs_need": r"Додатково:\s*(.*?)[\U0001F300-\U0001FAD6]",
            "schedule": r"Графік роботи:\s*(.*?)[\U0001F300-\U0001FAD6]",
            "apartment": r"Житло:\s*(.*?)[\U0001F300-\U0001FAD6]",
            "uniform": r"Спецодяг:\s*(.*?)[\U0001F300-\U0001FAD6]",
            "transfer": r"Трансфер на роботу:\s*(.*?)[\U0001F300-\U0001FAD6]",
            "age": r"Для кого:\s*(.*?)[\U0001F300-\U0001FAD6]",
            "experience": r"Досвід:\s*(.*?)[\U0001F300-\U0001FAD6]",
            "language": r"Знання мови:\s*(.*?)[\U0001F300-\U0001FAD6➕]",
            "duties": r"Обов’язки:\s*(.*?)[\U0001F300-\U0001FAD6]",
            "payment": r"Оплата чистими:\s*(.*?)[\U0001F300-\U0001FAD6]",
        }

        data = {key: extract_info(pattern, description) for key, pattern in patterns.items()}
        data["site"] = "www.group-working.com"

        # Обработка возраста и пола
        if data["age"]:
            age_numbers = re.findall(r'\d+', data["age"])
            
            if len(age_numbers) == 1 and "до" in data["age"]:
                age_numbers = [18, int(age_numbers[0])]
            elif len(age_numbers) == 1 and "від" in data["age"]:
                age_numbers = [int(age_numbers[0]), 55]
            
            data["min_age"] = int(age_numbers[0]) if age_numbers else 18
            data["max_age"] = int(age_numbers[1]) if len(age_numbers) > 1 else 55
            data["sex"] = data["age"].split(" ")[0]
        else:
            data["min_age"], data["max_age"], data["sex"] = 18, 55, ' '
        active = True

        # Сохранение данных в базу
        self.cursor.execute(
            """
            INSERT INTO vac_form_vacancy (
                vac_id, position, job_category, country, salary, date_posted, sex, 
                vaccity, docs_need, schedule, apartment, uniform, tools, transfer, age,
                min_age, max_age, experience, language, duties, payment, active, site
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vac_id) DO UPDATE SET 
                position = EXCLUDED.position,
                job_category = EXCLUDED.job_category,
                country = EXCLUDED.country,
                salary = EXCLUDED.salary,
                date_posted = EXCLUDED.date_posted,
                sex = EXCLUDED.sex,
                vaccity = EXCLUDED.vaccity,
                docs_need = EXCLUDED.docs_need,
                schedule = EXCLUDED.schedule,
                apartment = EXCLUDED.apartment,
                uniform = EXCLUDED.uniform,
                tools = EXCLUDED.tools,
                transfer = EXCLUDED.transfer,
                age = EXCLUDED.min_age,
                min_age = EXCLUDED.min_age,
                max_age = EXCLUDED.max_age,
                experience = EXCLUDED.experience,
                language = EXCLUDED.language,
                duties = EXCLUDED.duties,
                payment = EXCLUDED.payment,
                active = EXCLUDED.active,
                site = EXCLUDED.site
            """,
            (vac_id, position, job_category, country, salary, date_posted, data["sex"], vaccity,
             data["docs_need"], data["schedule"], data["apartment"], data["uniform"], tools, data["transfer"], 
             data["age"], data["min_age"], data["max_age"], data["experience"],
             data["language"], data["duties"], data["payment"], active, data["site"])
        )
        self.conn.commit()

    def closed(self, reason):
        """Закрывает подключение к базе данных при завершении работы паука."""
        self.cursor.close()
        self.conn.close()


