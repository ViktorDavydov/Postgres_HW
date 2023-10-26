import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'hw5_db'
    ini_config_file = 'database.ini'
    section_params = 'postgresql'

    params = config(ini_config_file, section_params)
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        connection = psycopg2.connect(**params)
        with connection as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    try:
        connection = psycopg2.connect(host=params['host'], user=params['user'],
                                      password=params['password'], port=params['port'])
        connection.autocommit = True
        cur = connection.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
        cur.execute(f"CREATE DATABASE {db_name};")

    finally:
        connection.close()


def execute_sql_script(cursor, fill_script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(fill_script_file, 'r') as file:
        cursor.execute(file.read())


def create_suppliers_table(cursor) -> None:
    """Создает таблицу suppliers."""
    cursor.execute("DROP TABLE IF EXISTS suppliers;")
    cursor.execute("CREATE TABLE suppliers"
                   "("
                   "    company_name varchar(50),"
                   "    contact varchar(100),"
                   "    address text,"
                   "    phone varchar(25),"
                   "    fax varchar(25),"
                   "    homepage text,"
                   "    products text"
                   ")")


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r', encoding='UTF-8') as source_file:
        data = json.load(source_file)
        return data


def insert_suppliers_data(cursor, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for item in suppliers:
        cursor.execute(f"INSERT INTO suppliers VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (item['company_name'], item['contact'], item['address'],
                        item['phone'], item['fax'], item['homepage'], ", ".join(item['products'])))


def add_foreign_keys(cursor, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    pass

if __name__ == '__main__':
    main()
