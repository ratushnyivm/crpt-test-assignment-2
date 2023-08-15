import datetime
import json
import os
import random
import uuid

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

inns = ['owner_1', 'owner_2', 'owner_3', 'owner_4']
status = [1, 2, 3, 4, 10, 13]
d_type = ['transfer_document', 'not_transfer_document']


class PsqlTools:
    def __init__(self):
        self.conn = {'host': os.getenv('POSTGRES_HOST', '127.0.0.1'),
                     'user': os.getenv('POSTGRES_USER', 'postgres'),
                     'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
                     'db': os.getenv('POSTGRES_NAME', 'test_migration')}        # заполнить название бд, не принципиально
        self.port = os.getenv('POSTGRES_PORT', '5432')
        self.schema = os.getenv('POSTGRES_SCHEMA', 'public')

    def query(self, q, list_conditions = None):
        xsql3_con = psycopg2.connect(host=self.conn.get('host'), dbname=self.conn.get('db'),
                                     user=self.conn.get('user'), password=self.conn.get('password'), port=self.port)
        xs3cur = xsql3_con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            if list_conditions:
                xs3cur.execute(q, list_conditions)
            else:
                xs3cur.execute(q)
            xsql3_con.commit()
            return True
        except Exception as e:
            print(f" ---- PSQL error: {e} ----")
            return None
        finally:
            xsql3_con.close()
            
    def insert(self, tbl: str, up: dict):
        tbl = f'{self.schema}.{tbl}' if self.schema else tbl
        f = ', '.join(list(up.keys()))
        vl = list()
        for x in range(len(up)):
            vl.append('%s')
        v = ', '.join(vl)
        q = f'insert into {tbl}({f}) values ({v})'
        return self.query(q, list(up.values()))


def make_data(_p: PsqlTools, commit=True) -> [dict, set]:
    parents = set()
    children = dict()
    data_table = dict()

    for _ in list(range(0, 20)):
        parents.add('p_' + str(uuid.uuid4()))

    for p in parents:
        children[p] = set()
        for _ in list(range(0, 50)):
            children[p].add('ch_' + str(uuid.uuid4()))

    for k, ch in children.items():
        data_table[k] = {'object': k,
                         'status': random.choice(status),
                         'owner': random.choice(inns),
                         'level': 1,
                         'parent': None}

        for x in ch:
            data_table[x] = {'object': x,
                             'status': random.choice(status),
                             'owner': data_table[k]['owner'],
                             'level': 0,
                             'parent': k}

    if commit:   
        for v in data_table.values():
            _p.insert('data', v)
    return data_table


def make_documents(_p: PsqlTools, data: dict, commit=True):
    saler = reciver = random.choice(inns)
    while saler == reciver:
        reciver = random.choice(inns)

    doc = dict()
    dd = doc['document_data'] = dict()
    dd['document_id'] = id = str(uuid.uuid4())
    dd['document_type'] = random.choice(d_type)

    doc['objects'] = [x for x, v in data.items() if v['level'] == 1 and v['owner'] == saler]

    md = doc['operation_details'] = dict()

    if random.choice([0, 1]):
        mds = md['status'] = dict()
        mds['new'] = mds['old'] = random.choice(status)
        while mds['old'] == mds['new']:
            mds['new'] = random.choice(status)

    if dd['document_type'] == d_type[0]:
        mdo = md['owner'] = dict()
        mdo['new'] = mdo['old'] = random.choice(inns)
        while mdo['old'] == mdo['new']:
            mdo['new'] = random.choice(inns)

    if commit:
        _p.insert('documents', {'doc_id': id,
                                'recived_at': datetime.datetime.now(),
                                'document_type': dd['document_type'],
                                'document_data': json.dumps(doc)})


def __make_tables(_p: PsqlTools):
    data_tbl = '''DROP TABLE IF EXISTS public.data;
                CREATE TABLE IF NOT EXISTS public.data
                (
                    object character varying(50) COLLATE pg_catalog."default" NOT NULL,
                    status integer,
                    level integer,
                    parent character varying COLLATE pg_catalog."default",
                    owner character varying(14) COLLATE pg_catalog."default",
                    CONSTRAINT data_pkey PRIMARY KEY (object)
                )'''
    _p.query(data_tbl)

    document_tbl = '''
                DROP TABLE IF EXISTS public.documents;
                CREATE TABLE IF NOT EXISTS public.documents
                (
                    doc_id character varying COLLATE pg_catalog."default" NOT NULL,
                    recived_at timestamp without time zone,
                    document_type character varying COLLATE pg_catalog."default",
                    document_data jsonb,
                    processed_at timestamp without time zone,
                    CONSTRAINT documents_pkey PRIMARY KEY (doc_id)
                )'''
    _p.query(document_tbl)

    """
    Подготовка: 
    Заполнить креды подключения к psql, название таблицы не принципиально. 
    Запустить этот скрипт для заполнения базы рандомными данными.
    
    Легенда: 
    есть таблица documents с условными документами, которые поступают от клиентов,
    есть таблица data с условными объектами, которые содержатся в документах, они могут быть связаны полем parent, 
    в этом случае условный объект считаем упаковкой, а дочерние элементы, 
    у которых он заполнен в поле parent - содержимым упаковки
    
    Тестовое задание:
    Написать алгоритм обработки документов из таблицы documents по условиям

    Структура json документа из поля document_data:
    {
        "document_data": {
            "document_id": "25e91d56-696e-4be6-952c-4089593877a7",
            "document_type": "transfer_document"
        },
        "objects": [
            "p_effe6195-cc7f-44c2-a02c-46fc07dcd3e6",
            "p_8943e9fb-a2e7-4344-8c48-91d3a4fbdb0c",
        ],
        "operation_details": {
            "owner": {
                "new": "owner_4",
                "old": "owner_3"
            }
        }
    }

    После запуска скрипта он должен брать 1 запись из таблицы documents (сортировка по полю recived_at ASC) по условиям:
        тип документа: transfer_document
        поле processed_at: is NULL 
    и обрабатывать содержимое поля document_data, которое содержит условное содержимое документа, по алгоритму:
    
        1. взять объекты из ключа objects
        2. собрать полный список объектов из таблицы data, учитывая, что в ключе objects содержатся объекты, у которых 
           есть связанные элементы (связь по полю parent таблицы datа)
        3. изменить данные для объектов в таблице data, если они подходят под условие блока operation_details в 
           document_data, где каждый ключ это название поля, внутри блок со старым значение в ключе old, которое нужно 
           проверить, и новое значение в ключе new, на которое нужно данные изменить
           пример: 
            "owner": {
                "new": "owner_4",
                "old": "owner_3"
            }
        4. после обработки документа в таблице documents поставить отметку времени в поле processed_at
        5. Если всё завершилось успешно, возвращаем True, если нет - False
        
    """

if __name__ == '__main__':
    _p = PsqlTools()
    __make_tables(_p)
    data = make_data(_p)
    doc_count = random.choice(list(range(3, 20)))
    i = 1
    while i <= doc_count:
        make_documents(_p, data)
        i += 1
