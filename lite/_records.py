'''This module contains the Table class

This module is not for general use. It is a part of "lite" extension.
This part contains all the functionalities to create and manipulate records.
'''
from sqlite3 import connect, OperationalError


class Records():
    '''Records operations'''
    def __init__(self, database):
        '''database: lite.DB obj'''
        self.database = database

    def insert(self, table: str, record: list, empty = None) -> bool:
        '''Insert a record into a specific table in the current DB

        Parameters:
        -----------
        table: str
        The name of the correpsond table.

        record: list
        A list contains all the fields of the record
        and their types dependent on the nature and the contents
        of the table itself.

        empty: (Optional)
        If there a missing value(s) (fields) in the record,
        then dynamically add an "empty" value for each missing one.

        Return:
        -------
        True if the operation is success, otherwise return False.'''
        if table not in self.database.TABLES:
            return False
        operation = True
        fields = len(self.database.columns.fields(table))
        while len(record) < fields:  # padding empty value(s) if necessary
            record.append(empty)
        values = (len(record) * "?,")[:-1]
        code = f"INSERT INTO {table} VALUES ({values})"
        with connect(self.database.current_db) as conn:
            cursor = conn.cursor()  # Create a cursor
            try:
                cursor.execute(code, record)
                conn.commit()
            except OperationalError:
                operation = False
        return operation


    def delete_record(self, table: str, where: str = None) -> bool:
        '''Delete record(s) from a table

        Parametrs:
        ----------
        table: str
        Table's name

        Where: str (Optional)
        Optional WHERE clause with DELETE query to delete the selected rows

        Description:
        ------------
        By default, all the records would be deleted. However, the user can
        control the deletion process by passing a specific "WHERE" clause.
        The "WEHRE" clause is a valid sqlite "WHERE" clause, but the
        WHERE keyword could be omitted.

        e.g: Delete all the records from the "company" table of all
             the emplyees who have retired (or those over the age of 65)
             A SQLite/SQL query to handle this task:
             DELETE FROM company WHERE age > 65

             To handle the same task with delete(), we should pass a "where"
             clause -(the "WHERE" keyword could be omitted):
             "WHERE age > 65" or simply "age > 65"

        Return:
        -------
        True if the process success, otherwise return False '''
        if table not in self.database.TABLES:
            return False
        if where:
            where = 'WHERE ' + where.lower().split('where')[1].strip()
        else:
            where = ''  # if where is None
        return self.database.execute(f'''DELETE FROM {table} {where}''')
