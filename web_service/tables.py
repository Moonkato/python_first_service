from db import connector, cursor
import psycopg2
import random
import asyncio
import asyncpg

def create_table(table_name):
    try:
        cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        query = '''CREATE TABLE  people(
                FIRST_NAME CHAR(20) NOT NULL,
                SECOND_NAME CHAR(40) NOT NULL,
                NUMBER INT NOT NULL
                )'''
        cursor.execute(query)
        connector.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error} \n"
              f"Ошибка при создании новой таблицы")

def table_drop(table_name):
    try:
        cursor.execute(f'''DROP TABLE IF EXISTS {table_name}''')
        connector.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error} \n"
              f"Ошибка при удалении таблицы")

def add_column_to_table(table_name, column_name, column_type):
    try:
        query = f'''ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};'''
        cursor.execute(query)
        connector.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error} \n"
              f"Ошибка при добавлении новой колонки в таблицу")


def insert_into_table(table_name, first_name, second_name, number):
    try:
        query = f'''INSERT INTO {table_name}(FIRST_NAME, SECOND_NAME, NUMBER, hash_code) VALUES (%s, %s, %s, %s)'''
        hash_code = random.getrandbits(128)
        cursor.execute(query,(first_name, second_name, number, hash_code))
        connector.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error} \n"
              f"Ошибка при добавлении нового пользователя в таблицу")

def update_amounts(table_name):
    try:
        query = f"SELECT first_name, second_name, number, hash_code FROM {table_name};"

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            first_name, second_name, number, hash_code = row

            number = random.randint(0,100)

            update_query = f"UPDATE {table_name} SET number = %s WHERE hash_code = %s;"
            cursor.execute(update_query, (number, hash_code))

        connector.commit()

    except(Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")


def update_table_values_by_updating_hash_to_each_row(table_name):
    try:
        select_query = f"SELECT first_name, second_name, number, hash_code FROM {table_name};"
        cursor.execute(select_query)
        rows = cursor.fetchall()

        for row in rows:
            first_name, second_name, number, hash_code = row

            hash_code = random.getrandbits(128)

            update_query = f"UPDATE {table_name} SET hash_code = %s WHERE first_name = %s AND second_name = %s AND number = %s;"
            cursor.execute(update_query, (hash_code, first_name, second_name, number))
        connector.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")

def find_user(table_name, hash_code):
    try:
        query = f"SELECT first_name, second_name, number, hash_code FROM {table_name} WHERE hash_code = %s;"
        cursor.execute(query, (hash_code,))

        result = cursor.fetchone()

        if result:

            first_name, second_name, number, my_hash_code = result
            return first_name, second_name
        else:
            return None, None

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}"
              f"Ошибка при поиске пользователя")

def get_certain_row_from_table(table_name, row_number):
    try:
        query = f"SELECT * FROM {table_name} OFFSET {row_number-1} LIMIT {1};"
        cursor.execute(query)

        result = cursor.fetchone()

        return result
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}"
              f"Ошибка при поиске {row_number} строки")

async def fill_the_list(row: list, answer_string: str):
    for index, smth in enumerate(row):
        if index != 2 and index != 3:
            answer_string += str(smth)
        elif index == 2:
            answer_string = answer_string + "[" + str(smth) + "] "

    return answer_string

async def change_certain_row(table_name, row):
    db_params = {
        'user': 'postgres',
        'password': '2552Moonkato',
        'database': 'people',
        'host': '127.0.0.1',
        'port': '5432'
    }
    async_connector = await asyncpg.connect(**db_params)

    amount = row[2]
    hash_code = row[3]
    new_amount = amount * 2
    query = f"UPDATE {table_name} SET number = $1 WHERE hash_code = $2;"


    await async_connector.execute(query, new_amount, hash_code)
    await async_connector.close()


async def change_first_n_rows(table_name, number_of_rows):
    try:
        answer_string = ""
        for counter in range(1, number_of_rows + 1):
            row = get_certain_row_from_table(table_name, counter)
            await change_certain_row("people", row)
            answer_string = await fill_the_list(row, answer_string)
        return answer_string

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}"
              f"Ошибка при поиске {number_of_rows} строк")


#update_amounts("people")
#insert_into_table("people", "Gleb", "Alektorov", 45)
#update_amounts("people")
#add_column_to_table("people", "hash_code", "CHAR(100)")
#update_table_values_by_updating_hash_to_each_row("people")


# first, second = find_user("people", "66817269009576936538171960556908463470                                                              ")
# print(first)

# cursor.close()
# connector.close()

