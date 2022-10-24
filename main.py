import csv
import requests
import psycopg2

URL = "https://randomuser.me/api/"
request_json = requests.get(URL)
response_json = request_json.json()

def get_data_from_api(URL):
    resp = requests.get(URL).json()
    if resp.status_code != 200:
        get_data_from_api(URL)
    return resp


response_json = get_data_from_api(URL)

with open("DB.csv", 'w') as file:
    writer = csv.writer(file)
    key_data = []
    column_data = []
    value_data = []

    for key, value in response_json.items():
        if isinstance(value, dict):
            for info_key, info_value in value.items():
                key_data.append(info_key)
                value_data.append(info_value)
        else:
            for result_json in value:
                for result_key, result_value in result_json.items():
                    if isinstance(result_value, dict):
                        for nested_key, nested_value in result_value.items():
                            if nested_key == 'date' and result_key == 'dob':
                                nested_key = 'dob_date'
                            if nested_key == 'age' and result_key == 'dob':
                                nested_key = 'dob_age'
                            if nested_key == 'name' and result_key == 'id':
                                nested_key = 'id_name'

                            if isinstance(nested_value, dict):
                                for final_key, final_value in nested_value.items():
                                    if final_key == 'offset' and result_key == 'location':
                                        final_key = 'of_set'
                                    key_data.append(final_key)
                                    value_data.append(final_value)
                            else:
                                key_data.append(nested_key)
                                value_data.append(nested_value)
                    else:
                        key_data.append(result_key)
                        value_data.append(result_value)

    for value in key_data:
        if isinstance(value, str):
            if "," in value:
                value = value.replace(",", "")

        column_data.append(value)

    writer.writerow(column_data)
    writer.writerow(value_data)

    hostname = 'localhost'
    database = 'newdb'
    username = 'postgres'
    pwd = 'postgres'
    port_id = 5432

    db_connection = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id)

    db_cursor = db_connection.cursor()
    create_table = """CREATE TABLE value_column(
                      );"""

    db_cursor.execute(create_table)
    for column in column_data:
        db_cursor.execute(f"""ALTER TABLE  value_column
                               ADD {column} varchar(120)
                            """)
    db_connection.commit()

open_csv = open("DB2.csv", "w")
k = csv.writer(open_csv)
k.writerow(value_data)
open_csv.close()


def copy_from_file(conn, table):
    with open("DB2.csv") as f:
        cursor = conn.cursor()
        cursor.copy_from(f, table, sep=",")
        conn.commit()
        cursor.close()


copy_from_file(db_connection, "value_column")
