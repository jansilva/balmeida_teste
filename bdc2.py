import requests
import json
import psycopg2

class DatabaseConnection:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(
                host='xxxx'
                , dbname= 'snowplow'       
                , port= '5439'
                , user= 'bruno_almeida'
                , password= 'xxx')

            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        except:
            print("Cannot connect to database")

    def execute_query(self):
        execute_query_command = ("SELECT id, app_codigo FROM pebmedapps.tb_usuario LIMIT 10;")
        self.cursor.execute(execute_query_command)
        rows = self.cursor.fetchall()
        print (f"id    | app_codigo ")
        print (f"------ ----------- ")
        for row in rows:
            print (f"{row[0]}        {row[1]} ")
        
        self.cursor.close()
        self.connection.close()

    def insert_record(self):
        new_record = ("10", "6")
        insert_command = ("INSERT INTO tablexxx (col1, col2) VALUES ('" + new_record[0] + "','" + new_record[1] + "') ")
        print(insert_command)
        self.cursor.execute(insert_command) 


if __name__ == '__main__':
    db_connection = DatabaseConnection()
    db_connection.execute_query()