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

    def __open_connect(self) -> None:
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
        self.__cursor.close()
        self.__connection.close()

    def select_one(self, q) -> tuple | None:
        self.__open_connect()
        if self.__connection:
            self.__cursor.execute(q)
            row = self.__cursor.fetchone()
            self.__connection.commit()
            self.__close_connect()
            return row

    def select_all(self, q) -> list[tuple] | None:
        self.__open_connect()
        if self.__connection:
            self.__cursor.execute(q)
            rows = self.__cursor.fetchall()
            self.__connection.commit()
            self.__close_connect()
            return rows

    def update_one(self, query: str, vars: dict = None):
        self.__open_connect()
        if self.__connection:
            self.__cursor.execute(query, vars)
            self.__connection.commit()
            self.__close_connect()
