"""
@file_name : SQLDatabaseManager.py
@author : Srihari Seshadri
@description : This file defines the class that connects to the database. It
                contains methods that allow CRUD operations.
                Requires interface with pandas library.
                Database connections are made by using the sqlalchemy library
                in tandem with the pymysql library
@date : 11-13-2018
"""

from sqlalchemy import create_engine, exc
import pandas as pd


class SQLDatabaseManager:

    def __init__(self):
        self._engine = None
        self._database = None
        self._host = None
        self._uname = None
        self._pwd = None
        self._port = None
        pass

    def connect(self, host, database, username, password, port):
        """
        Creates a connection with the database
        :param host: Host IP address
        :param database: Database/Schema name to connect to
        :param username: username for authentication
        :param password: password for username
        :param port: port number of the database connection.
        :return: 1 if success. <0 if failure
        """
        try:
            self._database = database
            self._uname = username
            self._pwd = password
            self._host = host
            self._port = port
            self._engine = create_engine("mysql+pymysql://" +
                                         self._uname + ":" +
                                         self._pwd + "@" +
                                         self._host + ":" +
                                         self._port + "/" +
                                         self._database)
            self._engine.connect()      # Is this line needed?
        except exc.SQLAlchemyError as err:
            print(" SQL Alchemy threw an error :")
            print(err)
            return -1
        except exc.DBAPIError as err:
            print(" A DBAPI error occured : ")
            print(err)
            return -2
        return 1

    def get_tables(self):
        try:
            return self._engine.table_names()
        except Exception as e:
            print("Exception thrown while retrieving table names : ", e)

    def execute_query(self, query):
        """
        Executes the SQL query as typed
        :param query: String of SQL query
        :return: A dataframe if success. -1 if fail.
        """
        try:
            df = pd.read_sql(query, con=self._engine)
        except Exception as e:
            print(" An exception was thrown : ")
            print(e)
            return -1
        return df

    def insert(self, dframe, table_name, if_table_exists="append"):
        """
        Inserts the new data frame into the specified table.
        :param dframe: Dataframe containing the data
        :param table_name: Table name
        :param if_table_exists: Flag to specify what to do if table already
            exists. Default - "append"
            - fail: If table exists, do nothing.
            - replace: If table exists, drop it, recreate it, and insert data.
            - append: If table exists, insert data. Create if does not exist.
        :return: 1 if Success. -1 if fail.
        """
        try:
            dframe.to_sql(name=table_name,
                          if_exists=if_table_exists,
                          con=self._engine,
                          schema=self._database,
                          index=False)
        except Exception as e:
            print(" An exception occured during Insert query")
            print(e)
            return -1
        return 1

    def disconnect(self):
        self._engine.dispose()


def main():
    sqldbm = SQLDatabaseManager()

    host = '35.202.176.53'
    database = 'crime_weather_2010_2018'
    user = 'srihari'
    password = 'Irahirs1@1338'
    port = '3306'

    ret = sqldbm.connect(host=host,
                         database=database,
                         username=user,
                         password=password,
                         port=port)

    if ret != 1:
        print(" Closing program ")
        return

    print(sqldbm.get_tables())


if __name__ == "__main__":
    main()
