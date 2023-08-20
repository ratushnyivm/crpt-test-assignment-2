import collections
import logging
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv('POSTGRES_HOST', '127.0.0.1')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'test_migration')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')


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


class ProcessDocument:
    """Класс для обработки документа и связанных с ним объектов."""

    document = None
    operation_details = None

    def __init__(self, db: PsqlManager) -> None:
        self.db = db

    def get_document(self) -> dict | None:
        """
        Получение документа для обработки.
        Возвращает словарь, где ключ - поле таблицы в базе данных,
        а значение - содержимое ячейки.
        """

        query = """
            SELECT doc_id, document_data
            FROM documents
            WHERE document_type = 'transfer_document' AND processed_at IS NULL
            ORDER BY recieved_at ASC
            LIMIT 1
        """
        row = self.db.select_one(query)
        if row:
            self.document = {
                'doc_id': row[0],
                'document_data': row[1]
            }
            logging.info(
                f"Документ [id = {self.document['doc_id']}] успешно получен"
            )
            return self.document

    def get_document_objects(self) -> list[str] | None:
        """
        Получение объектов документа без учёта дочерних.
        Возвращает список строк, которые являются идентификаторами объектов.
        """

        doc_objects = self.document.get('document_data').get('objects')
        if not doc_objects:
            logging.info(
                f"Документ [id = {self.document['doc_id']}] не содержит "
                f"ссылок на другие объекты"
            )
            return
        return doc_objects

    def get_valid_operation_details(self) -> dict[str, dict]:
        """
        Валидация деталей операций.
        Возвращает словарь, где ключ - подлежащее изменению поле объекта,
        а значение - словарь с ключами 'old' и 'new', указывающими на то,
        какие значения подлежат изменению.
        """

        self.operation_details = self.document.get('document_data')\
            .get('operation_details')

        for key, value in list(self.operation_details.items()):
            if not (value.get('old') and value.get('new')):
                del self.operation_details[key]

        if not self.operation_details:
            return

        logging.info("Поля, требующие изменений, успешно провалидированы")
        return self.operation_details

    def get_related_objects(self) -> list[dict] | None:
        """
        Получение всех связанных с документом объектов, которые удовлетворяют
        условиям деталей операций.
        Возвращает список из словарей для каждого объекта, где ключ - поле
        таблицы в базе данных, а значение - содержимое ячейки.
        """

        if not self.document:
            return

        doc_objects = self.get_document_objects()
        if not (doc_objects and self.operation_details):
            return

        # Генерация списка колонок, значения которых подлежат изменению, и
        # списка условий соответствия имеющихся значений в таблице дата
        # значениям old в operation details
        column_list = ['object']
        condition = []
        for k, v in self.operation_details.items():
            column_list.append(k)
            old_value = v['old']
            if isinstance(old_value, list):
                condition.append(f"{k} IN {tuple(v['old'])}")
            else:
                condition.append(f"{k} = '{v['old']}'")

        columns = ', '.join(column_list)
        condition = ' OR '.join(condition)

        query = f"""
            SELECT {columns}
            FROM data
            WHERE (object IN %(doc_objects)s OR parent IN %(doc_objects)s)
                AND ({condition})
        """
        vars = {'doc_objects': tuple(doc_objects)}
        rows = self.db.select_all(query, vars)

        if not rows:
            logging.info('Объекты, требующие изменений, не найдены')
            return

        related_objects = []
        keys = column_list
        for row in rows:
            obj = dict()
            for i in range(len(keys)):
                obj[keys[i]] = row[i]
            related_objects.append(obj)
        logging.info('Объекты, требующие изменений, успешно получены')
        return related_objects

    def update_related_objects(self) -> bool | None:
        """Обновление всех связанных с документом объектов."""

        related_objects = self.get_related_objects()
        if not (related_objects and self.operation_details):
            logging.info(
                'Обновление пропущено, так как отсутствуют подходящие объекты '
                'и/или детали операции.'
            )
            return

        # Аккумулятор списка объектов для каждой изменяемой колонки
        acc = collections.defaultdict(list)
        for obj in related_objects:
            for k, v in self.operation_details.items():
                if obj[k] == v['old']:
                    acc[k].append(obj['object'])

        for column, objects in acc.items():
            query = f"""
                UPDATE data
                SET {column} = %(new_value)s
                WHERE object IN %(objects)s
            """
            vars = {
                'new_value': self.operation_details[column]['new'],
                'objects': tuple(objects)
            }
            self.db.update_one(query, vars)

        logging.info(
            f"Все объекты, связанные с документом "
            f"[id = {self.document['doc_id']}], успешно обновлены"
        )

    def update_document(self) -> None:
        """Установка времени обновления документа."""

        query = """
            UPDATE documents
            SET processed_at = now()
            WHERE doc_id = %(doc_id)s
        """
        vars = {'doc_id': self.document['doc_id']}
        self.db.update_one(query, vars)
        logging.info(
            f"Дата и время обновления документа "
            f"[id = {self.document['doc_id']}] успешно установлены"
        )


def main() -> bool:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s :: %(levelname)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    db = PsqlManager(
        db=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    doc_process = ProcessDocument(db)
    try:
        document = doc_process.get_document()
        if document:
            doc_process.get_valid_operation_details()
            doc_process.update_related_objects()
            doc_process.update_document()
            return True
        logging.info('Документ, требующий внесения изменений, не обнаружен')
        return False
    except Exception as e:
        logging.error(e)
        return False


if __name__ == '__main__':
    while True:
        if not main():
            break
    logging.info('Выполнение скрипта остановлено')
