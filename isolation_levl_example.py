import pandas
import threading
import os
from threading import Thread
# from EsunPostgresql import Postgresql
from sqlalchemy import create_engine

def create_table(table_name, schema='public'):
    strsql_create = f'DROP TABLE IF EXISTS {table_name} ; \
            CREATE TABLE IF NOT EXISTS "{table_name}" ( \
            id varchar(10) NOT NULL, \
            grade float NOT NULL );'

    return strsql_create


def create_table2(table_name, schema='public'):
    strsql_create = f'DROP TABLE IF EXISTS {table_name} ;\
            CREATE TABLE IF NOT EXISTS {table_name} ( \
            id int NOT NULL, \
            color varchar(10) NOT NULL );'

    return strsql_create


def init_data(table_name):
    sql_str = f'insert into {table_name}' \
        f'("id", "grade") VALUES' \
        f'(\'AAA\', 99),' \
        f'(\'DDD\', 98),' \
        f'(\'ZZZ\', 80),' \
        f'(\'XXX\', 60);'
    return sql_str


def init_data2(table_name):
    sql_str = f'insert into {table_name}' \
        f'("id", "color") VALUES'

    for i in range(10):
        sql_str = sql_str + f"({i}, '{'white' if i%2==0 else 'black'}'),"
    sql_str = sql_str.rstrip(sql_str[-1])
    sql_str = sql_str + ';'
    return sql_str


def Case1_Nonrepeatable_Read(A_connection, B_connection):
    create_table_str = create_table('exam_grade')
    for sql_str in create_table_str.split(";"):
        if sql_str == '':
            continue
        A_connection.execute(sql_str)
    init_data_str = init_data('exam_grade')
    B_connection.execute(init_data_str)
    # A_str_sql="begin transaction isolation level repeatable read ;"
    # A_connection.execute(A_str_sql)
    print('---Init---')
    A_str_sql = "select * from exam_grade where id='AAA';"
    re = A_connection.execute(A_str_sql)
    for row in re:
        print(row)
    B_str_sql = "update exam_grade set grade=50 where id='AAA';"
    B_connection.execute(B_str_sql)
    print('---Second query ---')

    re = A_connection.execute(A_str_sql)
    for row in re:
        print(row)


def Case2_Phantom_Read(A_connection, B_connection):
    create_table_str = create_table('exam_grade')
    for sql_str in create_table_str.split(";"):
        if sql_str == '':
            continue
        A_connection.execute(sql_str)
    init_data_str = init_data('exam_grade')
    B_connection.execute(init_data_str)
    print('---Init---')
    A_str_sql = "select * from exam_grade;"
    re = A_connection.execute(A_str_sql)
    for row in re:
        print(row)
    B_str_sql = 'insert into exam_grade ("id", "grade") VALUES (\'YYY\', 60);'

    print('---Second query ---')
    B_connection.execute(B_str_sql)
    re = A_connection.execute(A_str_sql)
    for row in re:
        print(row)


def Case3_Serialization_Anomaly(A_connection, B_connection):

    create_table_str = create_table2('dots')
    for sql_str in create_table_str.split(";"):
        if sql_str == '':
            continue
        A_connection.execute(sql_str)
    init_data_str = init_data2('dots')
    B_connection.execute(init_data_str)

    print('---Init---')
    A_str_sql = "select * from dots order by id;"
    re = A_connection.execute(A_str_sql)
    for row in re:
        print(row)

    thread1 = Thread(target=Case3_Serialization_Anomaly_A,
           args=(A_connection, ),
           daemon=True)
    thread2 = Thread(target=Case3_Serialization_Anomaly_B,
           args=(B_connection, ),
           daemon=True)
    print('---Concurrent---')
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    A_str_sql = "select * from dots order by id;"
    re = A_connection.execute(A_str_sql)
    for row in re:
        print(row)


def Case3_Serialization_Anomaly_A(A_connection):
    A_str_sql = "update dots set color='black' where color='white';"
    A_connection.execute(A_str_sql)


def Case3_Serialization_Anomaly_B(B_connection):
    B_str_sql = "update dots set color='white' where color='black';"
    B_connection.execute(B_str_sql)


def get_engine(db_type):
    engine = None
    if db_type == 'postgresql':
        host = '10.240.201.32'
        port = '5432'
        user = 'quant'
        password = 'quant'
        database = 'Testing'
        table_name = 'exam_grade'
        engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}", pool_recycle=3600)
    elif db_type == 'sqlite':
        file_path = os.path.dirname(os.path.abspath(__file__))
        sqlite_path = os.path.join(file_path, 'lock_test.db')
        engine = create_engine(f'sqlite:///{sqlite_path}')
    return engine

def pick_test(test):
    test_dict = {
        "repeatable_read":Case1_Nonrepeatable_Read,
        "phantom_read":Case2_Phantom_Read,
        "serialization_anomaly":Case3_Serialization_Anomaly
    }
    return test_dict[test]


if __name__ == "__main__":
    db_type_list = [
        # "postgresql", 
        "sqlite",
    ]
    isolation_level_list = [
        # "AUTOCOMMIT", 
        # "READ UNCOMMITTED", 
        "READ COMMITTED", 
        "REPEATABLE READ",
        "SERIALIZABLE",
    ]
    test_list = [
        "repeatable_read", 
        # "phantom_read", 
        # "serialization_anomaly"
    ]
    for db_type in db_type_list:
        engine = get_engine(db_type)
        for isolation_level in isolation_level_list:
            for test in test_list:
                print('---------------')
                print(f'Database Type:   {db_type}')
                print(f'Isolation Level: {isolation_level}')
                print(f'Test:            {test}')

                try:
                    sql_con_A = engine.connect().execution_options(
                        isolation_level=isolation_level)
                    sql_con_B = engine.connect().execution_options(
                        isolation_level=isolation_level)                    
                    pick_test(test)(sql_con_A, sql_con_B)
                    sql_con_A.close()
                    sql_con_B.close()
                except Exception as e:
                    print(str(e))
                

                print('---------------')
                


