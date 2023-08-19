import collections
import logging

import psycopg2

import config


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

    def update(self, q):
        self.__open_connect()
        if self.__connection:
            self.__cursor.execute(q)
            self.__connection.commit()
            self.__close_connect()


class ProcessDocument:

    document = None
    operation_details = None

    def __init__(self, db: PsqlManager) -> None:
        self.db = db

    def get_document(self) -> None:
        sql_query = """
            SELECT doc_id, document_data
            FROM documents
            WHERE document_type = 'transfer_document' AND processed_at IS NULL
            ORDER BY recieved_at ASC
            LIMIT 1
        """
        row = self.db.select_one(sql_query)
        if row:
            self.document = {
                'doc_id': row[0],
                'document_data': row[1]
            }
            logging.info(f"Получен документ [id = {self.document['doc_id']}]")
            return self.document

    def get_document_objects(self) -> list:
        doc_objects = self.document.get('document_data').get('objects')
        return doc_objects

    def get_valid_operation_details(self) -> dict[str, dict]:
        """Получить детали, у которых присутствуют поля old и new."""

        self.operation_details = self.document.get('document_data')\
            .get('operation_details')

        for key, value in list(self.operation_details.items()):
            if not (value.get('old') and value.get('new')):
                del self.operation_details[key]
        logging.info("Поля, требующие изменений успешно провалидированы")
        return self.operation_details

    def get_related_objects(self) -> list[dict] | None:
        if not self.document:
            return

        doc_objects = tuple(self.get_document_objects())

        # Генерация условия соответствия колонок в таблице data
        # полям old в operation details
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

        sql_query = f"""
            SELECT {columns}
            FROM data
            WHERE (object IN {doc_objects} OR parent IN {doc_objects})
                AND ({condition})
        """
        rows = self.db.select_all(sql_query)

        if not rows:
            logging.info('Объекты, требующие изменений не найдены')
            return

        related_objects = []
        keys = column_list
        for row in rows:
            obj = dict()
            for i in range(len(keys)):
                obj[keys[i]] = row[i]
            related_objects.append(obj)
        logging.info('Объекты, требующие изменений успешно получены')
        return related_objects

    def update_related_objects(self):
        related_objects = self.get_related_objects()
        if not related_objects or not self.operation_details:
            return

        # Аккумулятор объектов (полей object) по колонкам,
        # требующим внесения изменений
        acc = collections.defaultdict(list)
        for obj in related_objects:
            for k, v in self.operation_details.items():
                if obj[k] == v['old']:
                    acc[k].append(obj['object'])

        for column in acc:
            sql_query = f"""
                UPDATE data
                SET {column} = '{self.operation_details[column]['new']}'
                WHERE object IN {tuple(acc[column])}
            """
            # print(sql_query)
            self.db.update(sql_query)
        logging.info(
            f"Все объекты, связанные с документом "
            f"[id = {self.document['doc_id']}] "
            f"успешно обновлены"
        )
        return True

    def update_document(self):
        document_id = self.document['doc_id']
        sql_query = f"""
            UPDATE documents
            SET processed_at = now()
            WHERE doc_id = '{document_id}'
        """
        self.db.update(sql_query)
        logging.info(
            f"Дата и время обновления документа "
            f"[id = {self.document['doc_id']}] успешно установлены"
        )


def main() -> bool:
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s :: %(levelname)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    db = PsqlManager(
        db=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT
    )
    doc_process = ProcessDocument(db)
    try:
        document = doc_process.get_document()
        if document:
            doc_process.get_valid_operation_details()
            doc_process.update_related_objects()
            doc_process.update_document()
            return True
        logging.info('Документ, требующий внесения изменений не обнаружен')
        return True
    except RuntimeError as e:
        logging.error(e)
        return False


if __name__ == '__main__':
    result = main()
    print(result)
