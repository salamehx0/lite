#!/usr/bin/env python3
'''Lite - a lighweight and powerful SQLite extension'''
from os import remove
from os.path import exists
from sqlite3 import connect, OperationalError
from ._tables import Tables
from ._columns import Columns
from ._records import Records


GENERAL = ("NULL", "INTEGER", "REAL", "TEXT", "BLOB")
AFFINITY = {  # SQLite Data Types

    'INTEGER':
        ('INTEGER', 'TINYINT', 'SMALLINT', 'MEDIUMINT', 'BIGINT',
         'UNSIGNED BIG INT', 'INT2', 'INT8'),

    'TEXT':
        ('CHARACTER(20)', 'VARCHAR(255)', 'VARYING CHARACTER(255)',
         'NCHAR(55)', 'NATIVE CHARACTER(70)', 'NVARCHAR(100)',
         'TEXT', 'CLOB'),

    'NONE': ('BLOB',
             ''),  # '' <==> no datatype specified

    'REAL': ('REAL', 'DOUBLE', 'DOUBLE PRECISION', 'FLOAT'),

    'NUMERIC': ('DECIMAL(10,5)', 'BOOLEAN', 'DATE', 'DATETIME')
}

class DB():
    '''Create/Manipulate a SQLite DB'''
    def __init__(self, name: str):
        self.current_db = name
        self.new_db(name)
        self.tables = Tables(self)
        self.columns = Columns(self)
        self.records = Records(self)


    # Specific Attributes
    @property
    def schema(self) -> dict:
        '''Get the schema of the current active DB

        Return a dict contains the tables' names and the names of their columns.
        format: {table1: [column1, column2, ..., columnN], ...,\
                 tableN: [column1, column2, ..., columnN,]}'''
        data = {}
        for table in self.TABLES:
            data[table] = self.columns.fields(table)
        return data

    @property
    def TABLES(self) -> list:
        '''A list contains the name of the tables in the current active DB'''
        return [i[0] for i in self.query("SELECT name FROM sqlite_master")]


    # Section 1: DB operations
    def new_db(self, name: str) -> bool:
        '''Create a new SQLite database
        Return True if the database is created, otherwise return False.

        Parameters:
        -----------
        name: str
        The name of the database.

        Description:
        ------------
         Firstly, check if their already an existed file with the same name
        of the DB wich we want to create. If no file exists the DB will be
        created and the current active DB (self.current_db) will be the
        created DB.

        Return:
        -------
        True if the process success, otherwise return False'''
        if exists(name):
            return False
        with connect(name) as conn:
            conn.cursor()  # Create a cursor
        self.current_db = name
        return True


    def drop_db(self, database) -> bool:
        '''Drop or delete a database

        |!| Note: the process will failed if the DB is not exist.
            (Close the DB connection before trying to delete it).

        Return True if the process success, otherwise return False
        '''
        if not exists(database):
            return False
        remove(database)
        return True


    def fetch(self, table: str, get: str = '*') -> list:
        '''Fetch data from the current active DB

        Parameters:
        -----------
        table: str
        The table name.

        Description:
        ------------
        get: str (Optional)
        Select the number of returned records. There are three available
        options:
            1) '*', fetch all the data from the DB
            2) '1', fetch only one record from the DB
            3) 'N', fetch 'N' records from the DB, where 'N' is
        		 	integer represents the number of the records

        Return:
        -------
        Return the a list contains the fetched records if the process success,
        otherwise return False.

        Note: If you try to fetch records from a table that does not exist,
            then the function will ignore process and return False.'''
        if table not in self.TABLES:
            return False
        msg = f'Invalid Option: "{get}"\nAvailable options: "*", "1", "N"\n'
        msg += 'for more info try help(obj.fetch)'
        conv = None
        try:
            conv = int(get)
        except ValueError:
            pass
        assert get in ('*', '1') or isinstance(conv, int), msg

        with connect(self.current_db) as conn:
            cursor = conn.cursor()  # Create a cursor
            cursor.execute(f"SELECT * FROM {table}")
            operation = {'*': cursor.fetchall, '1': cursor.fetchone,
                         'int': cursor.fetchmany}

            query = None
            try:
                if isinstance(get, str) and get in ('*', '1'):
                    func = operation[get]
                    query = [func()] if get == '1' else func()
                else:
                    func = operation['int']
                    query = func(int(get))
                conn.commit()
            except OperationalError:
                query = False
        return query


    def query(self, statement: str) -> list:
        '''Execute a specific SQL query.

        Parameters:
        -----------
        statement: str
        A SQL query.

        Description:
        ------------
        Use this function only for execute specific queries for fetching (or
        extracting) the DB instead than fetch().

        DO NOT USE this function to manipulate (e.g: change, delete, update)
        the DB to avoid some potentiel errors, instead you can use execute().

        Return:
        -------
        Return a list contains the output of the executed statement (if the
        statement is valid), otherwise return False.'''
        with connect(self.current_db) as conn:
            cursor = conn.cursor()  # Create a cursor

            try:
                cursor.execute(statement)
                query = cursor.fetchall()
            except OperationalError:
                query = False
        return query


    def execute(self, statement: str) -> bool:
        '''Manipulate the current DB

        Parameters:
        -----------
        statement: str
        A SQL query.

        Description:
        ------------
        Use this function for manipulating the DB, by manipulate we means
        executing SQL for operations like create, update, delete TABLES,
        or records.

        DO NOT USE it for fetching data from the DB, instead you can use
        either fetch() or query() which enables you to fech data more
        dynamically than fetch().

        ::Warning:: If you use this function to execute specific queries for
        fetching the DB, there is no guarantee that the result will be
        returned or even that the statement will be executed (depending on want
        you try to do with the DB).

        Return:
        -------
        True if the operation success, otherwise return False.'''
        operation = True
        with connect(self.current_db) as conn:
            cursor = conn.cursor()  # Create a cursor
            try:
                cursor.execute(statement)
                conn.commit()
            except OperationalError:
                operation = False
        return operation


    def clear(self):
        '''Drop all the tables in the current DB'''
        database = self.current_db
        self.drop_db(database)
        self.new_db(database)