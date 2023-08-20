import logging

import config
import database
import process_document


def main() -> bool:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s :: %(levelname)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    db = database.PsqlManager(
        db=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT
    )
    doc_process = process_document.ProcessDocument(db)
    try:
        document = doc_process.get_document()
        if document:
            doc_process.get_valid_operation_details()
            doc_process.update_related_objects()
            doc_process.update_document()
            return True
        logging.info('Документ, требующий внесения изменений, не обнаружен')
        return True
    except RuntimeError as e:
        logging.error(e)
        return False


if __name__ == '__main__':
    result = main()
    print(result)
