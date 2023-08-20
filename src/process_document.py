import collections
import logging

import database


class ProcessDocument:
    """Класс для обработки документа и связанных с ним объектов."""

    document = None
    operation_details = None

    def __init__(self, db: database.PsqlManager) -> None:
        self.db = db

    def get_document(self) -> dict | None:
        """
        Получение документа для обработки.
        Возвращает словарь, где ключ - поле таблицы в базе данных,
        а значение - содержимое ячейки.
        """

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
            logging.info(
                f"Документ [id = {self.document['doc_id']}] успешно получен"
            )
            return self.document

    def get_document_objects(self) -> list[str]:
        """
        Получение объектов документа без учёта дочерних.
        Возвращает список строк, которые являются идентификаторами объектов.
        """

        doc_objects = self.document.get('document_data').get('objects')
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
        # print(sql_query)
        rows = self.db.select_all(sql_query)

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
        if not related_objects or not self.operation_details:
            logging.info(
                'Обновление пропущено, так как отсутствуют подходящие объекты \
                 и/или детали операции.'
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
