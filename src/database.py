import logging

import psycopg2


class PsqlManager:
    """"Менеджер для работы с PostgreSQL."""

    __connection = None
    __cursor = None

    def __init__(self,
                 db: str,
                 user: str,
                 password: str,
                 host: str = 'localhost',
                 port: str = '5432') -> None:
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def select_one(self, query: str) -> tuple | None:
        """Получение одной записи из базы данных, возвращает кортеж."""

        self.__open_connect()
        if self.__connection:
            self.__cursor.execute(query)
            row = self.__cursor.fetchone()
            self.__connection.commit()
            self.__close_connect()
            return row

    def select_all(self, query: str, vars: dict) -> list[tuple] | None:
        """Получение всех записей из базы данных, возвращает список кортежей."""

        self.__open_connect()
        if self.__connection:
            self.__cursor.execute(query, vars)
            rows = self.__cursor.fetchall()
            self.__connection.commit()
            self.__close_connect()
            return rows

    def update_one(self, query: str, vars: dict) -> None:
        """Отправление одного запроса на обновление в базу данных."""

        self.__open_connect()
        if self.__connection:
            self.__cursor.execute(query, vars)
            self.__connection.commit()
            self.__close_connect()

    def __open_connect(self) -> None:
        """Открытие соединения с базой данных и получение курсора."""

        try:
            self.__connection = psycopg2.connect(
                dbname=self.db,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.__cursor = self.__connection.cursor()
        except psycopg2.OperationalError as e:
            logging.error(e)
            return

    def __close_connect(self) -> None:
        """Закрытие курсора и соединения с базой данных."""

        self.__cursor.close()
        self.__connection.close()
