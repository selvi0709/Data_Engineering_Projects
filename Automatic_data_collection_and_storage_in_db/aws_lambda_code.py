import requests
import json
import pymysql

db_host = 'dbname.amazonaws.com'
db_user = '****'
db_password = '****'
db_name = 'web'
db_port = 3306


def database_connect_check():
    print("started establishing connection")
    conn = pymysql.connect(host=db_host,
                           port=db_port,
                           user=db_user,
                           password=db_password,
                           database=db_name)
    print("successfully connected to db")
    conn.commit()
    conn.close()


def database_create():
    print("func in create")
    conn = pymysql.connect(host=db_host,
                           port=db_port,
                           user=db_user,
                           password=db_password)
    print("established")
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE web")
    conn.commit()
    conn.close()


def table_create():
    conn = pymysql.connect(host=db_host,
                           port=db_port,
                           user=db_user,
                           password=db_password,
                           database=db_name)
    table_name = 'api_data'
    table_exists = False
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        if result:
            table_exists = True
    if not table_exists:
        cursor = conn.cursor()
        sql = '''
        CREATE TABLE api_data(
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp INT NOT NULL,
            latitude VARCHAR(255) NOT NULL,
            longitude VARCHAR(255) NOT NULL,
            message VARCHAR(255) NOT NULL
        );
        '''
        cursor.execute(sql)
        conn.commit()
        print("Table created successfully")
    else:
        print("Table already exists")
    conn.close()


def table_delete():
    conn = pymysql.connect(host=db_host,
                           port=db_port,
                           user=db_user,
                           password=db_password,
                           database=db_name)
    table_name = 'web_data'
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.commit()
    print("Table deleted successfully.")
    conn.close()


def database_insert(data):
    conn = pymysql.connect(host=db_host,
                           port=db_port,
                           user=db_user,
                           password=db_password,
                           database=db_name)
    query = "INSERT INTO api_data (timestamp, latitude, longitude, message) VALUES (%s, %s, %s, %s)"
    for item in data:
        values = (
            item['timestamp'], item['iss_position']['latitude'], item['iss_position']['longitude'], item['message'])
        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()
    conn.close()


def database_retrieve():
    conn = pymysql.connect(host=db_host,
                           port=db_port,
                           user=db_user,
                           password=db_password,
                           database=db_name)
    with conn.cursor() as cur:
        cur.execute("select * from api_data")
        res = cur.fetchall()
        print("Printing records")
        for x in res:
            print(x)
        cur.execute("select count(*) from api_data")
        print("Total records on DB :" + str(cur.fetchall()[0]))
    conn.commit()
    conn.close()


def lambda_handler(event, context):
    # database_create()
    # print("DB created successfully")
    database_connect_check()
    # table_delete()
    table_create()
    result = []
    print('trying to connect to url')
    url = "http://api.open-notify.org/iss-now.json"
    response = requests.get(url)

    data = response.content
    result.append(json.loads(data.decode('utf-8')))

    print("Trying to insert")
    database_insert(result)
    print("Insertion done")

    print("Trying to retrieve")
    database_retrieve()
    print("retrieve done")

    print("Successfully completed")

    return {
        'statusCode': 200,
        'body': result
    }
