'''This module contains the Column class

This module is not for general use. It is a part of "lite" extension.
This part contains all the functionalities to create and manipulate columns.
'''
from sqlite3 import OperationalError, IntegrityError
from .lite import GENERAL, AFFINITY


class Columns():
    '''Columns operations'''
    def __init__(self, database):
        '''database: lite.DB obj'''
        self.database = database


    def add(self, table: str, column: str, unique: bool=False,
        null: bool=False, ftype: str = "TEXT") -> bool:
        '''Add a new column to the table

        Parameters:
        -----------
        table: str

        column: str
        column name

        ftype: str (Optional)
        The data type of the fields (Default TEXT)

        unique: bool (OPtional)
        Add a UNIQUE Constains to the field.

        null: bool (Optional)
        Allow the field to accept NULL values.

        Description:
        ------------
        Add a new column to the selected table, and specify (optional) the
        data type of the fields of that column.

        "ftype" could be one of the supported SQLite data type:
        ("NULL", "INTEGER", "REAL", "TEXT", "BLOB")

        Return:
        -------
        True if the process success, otherwise False'''
        if table not in self.database.TABLES or \
            column in self.fields(table):
            return False

        code = f'''ALTER TABLE {table} ADD COLUMN "{column}" {ftype}'''
        code += ' NULL' if null else ' NOT NULL'
        code += ' UNIQUE;' if unique else ';'
        return self.database.execute(code)


    def remove(self, table: str, column: str) -> bool:
        '''Remove or Delete a column from a table

           Return True if the process success, otherwise return True

           Note: If "table" is already has only a single column which
           is "column" then "table" will be dropped (same as drop())'''
        if table not in self.database.TABLES:
            return False

        fields = list(self.fields(table))
        if column not in fields:
            return False

        fields.remove(column)
        if len(fields) == 0:
            self.database.tables.drop(table)
            return True

        # fetch the fields data from "table" except "column"
        code = '''SELECT'''
        for field in fields:
            code += f" {field},"
        code = code[:-1]  # Remove the addionnal comma ','
        code += f" FROM {table}"
        records = self.database.query(code)

        # Drop the orginal table
        self.database.tables.drop(table)

        # Create a new table and insert the data
        self.database.tables.create(table, fields)
        for record in records:
            self.records.insert(table, record)
        return True


    def fields(self, table: str, types: bool = False) -> list:
        '''Return the names of all the fields of a table

        Parameters:
        -----------
        table: str
        Table's name

        types: bool (Optional)
        Also return the data type of each field

        Descreption:
        ------------
        Create a list that will contains the columns' names, each name is
        passed as a str value. If types is set to True, then the list will
        conains a set of tuples and each tuple contains respectively a column's
        name and the data type of its fields.

        Retrun:
        -------
        If "table" is not exists in the current active DB, then return a tuple
        that contains only a "None" (NoneType) value, otherwise return
        as mentioned in the description section'''
        if table not in self.database.TABLES:
            return (None,)

        query = f'SELECT name FROM PRAGMA_TABLE_INFO("{table}");' \
                if not types else \
                f'SELECT name, type FROM PRAGMA_TABLE_INFO("{table}");'

        fields = self.database.query(query)
        fields = [i[0] for i in fields] \
                 if not types else \
                 [(i[0], i[1]) for i in fields]
        return fields


    def primary_key(self, table, column, ftype="INTEGER",
        debug=False) -> bool:
        '''Create a new primary for an existed table

        Parameters:
        -----------
        table: str
        Table's name

        column: str
        Column's name

        ftype: str
        Field's type, this must be a valid
        "ftype" could be one of the supported SQLite data type:
        ("NULL", "INTEGER", "REAL", "TEXT", "BLOB")

        debug: bool (Default False)
        Enable debugging mode: Debug messages will be displayed in the
        terminal.

        Description:
        ------------
        If column not exist in "table", then create a new field with the
        name equal to "column" as make it as a primary key. If "column"
        is already exists in the database then it will be a primary key,
        but the order of the table will be changed ("column" will be
        the first column in the table)
        '''

        # Step 1: Check if there's already an existing column with the same
        # name, if there's make sure to remove it from "columns" because
        # columns will be inserted later into the the new table, and we
        # do not want duplication.
        columns = self.fields(table)
        if len(columns) == 1 and columns[0] is None:
            if debug:
                print(f'Error: "{table}" table is not exists')
            return False

        rowid = column if column in columns else 'ROWID'

        # Step 2: Get the names of all the current fields and their type          
        columns = dict(self.fields(table, True))
        if rowid != 'ROWID':
            del columns[column]

        # Step 3: Rename the table
        self.database.tables.rename(table, f"old_{table}")

        # Step 4: Create a new table with the primary key
        # and insert all the necessary records from the
        # orginal table
        execute =  self.database.execute
        def check(code, debug=False) -> bool:
            operation = True
            try:
                operation = execute(code)
            except (IntegrityError, OperationalError) as e:
                if debug:
                    print(e)
                operation = False
            return operation

        # We can only execute one statement at a time
        code = 'PRAGMA foreign_keys=off;'
        if not check(code, debug): return False

        code = 'PRAGMA foreign_keys=off;'
        if not check(code, debug): return False

        # code = f'ALTER TABLE {table} RENAME TO old_{table};'
        # if not check(code, debug): return False

        code = f'''
            CREATE TABLE {table} (
               {column} {ftype} NOT NULL PRIMARY KEY,
            '''
        for column in columns:  # Add all the columns in to new table
            ctype = columns[column]
            code += \
            f'''{column} {ctype} NOT NULL,'''
        code = code[:-1]  # Remove the addionnal comma ','
        code += ");"
        if not check(code, debug): return False

        # Insert all the records from the orginal table to the new one
        columns = list(columns)
        columns = f"{columns[0]}, " + ', '.join(columns[1:])
        code = \
        f'''
        INSERT INTO {table}
        SELECT {rowid}, {columns} FROM old_{table};
        '''
        if not check(code, debug):
            self.database.tables.drop(table)
            self.database.tables.rename(f"old_{table}", table)
            return False

        # Drop the orginal table (if operation is successful)
        self.database.tables.drop(f"old_{table}")
        return True


    def unique(table: str, column: str):
        '''Add a UNIQUE constraint to an existed column'''
        ftype = dist(self.columns(table, True))[table]
        self.database.execute(
            f'''ALTER TABLE {table}
                MODIFY {column} {ftype} NOT NULL UNIQUE;'''
        )