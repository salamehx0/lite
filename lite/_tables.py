'''This module contains the Table class

This module is not for general use. It is a part of "lite" extension.
This part contains all the functionalities to create and manipulate tables.
'''
from sqlite3 import connect, OperationalError
from .lite import GENERAL, AFFINITY


# Debuging mode function
def debug_mode(debug: bool, msg: str):
    '''print msg if msg debug is True'''
    if debug:
        print(f"Error: {msg}")


# Main code
class Tables():
    '''Tables Operations'''
    def __init__(self, database):
        '''database: lite.DB obj'''
        self.database = database


    def create(self, table: str, fields: list, auto: bool=True, null: list=tuple(),
        pk: str=None, uniques: list=tuple(), debug=False) -> bool:
        '''Create a new table

        Patameters:
        -----------
        table: str
        The table's name

        fields: list
        The filed of the inserted table.

        auto: bool (Default True)
        Automatically create an integer primmary key field called "id".

        pk: str (Default None)
        Specify a primary key.

        uniques: list (Optional)
        A list that contains the unique fields (each field will be has an
        "UNIQUE Constraint" which prevents two records from having identical
        values in a column).

        null: list (Optional)
        A list that contains all the fields that does not accept null values.
        By default all the fields does not accept null values.

        debug: bool (Default False)
        Enabling th debug mode: error messages will be displayed in the
        terminal.

        Description:
        ------------
        We can pass the fields in one of the following forms:

          1. A list of string, that contains the fields' names.
            format: ["field1", "field2", ..., "fieldN"]

          2. A list of tuple, that contains the fields'names and their
          respective types. The 1st element of a tuple, represent a
          "field-name", and the last represent "field-type".

           format: [("field1", type), ("field2", type), ..., ("fieldN", type)]

        e.g: create('employer', ('name', 'age', 'email'))
             The previous command will create a table called 'employer' by
            generating and executing the following code:

             CREATE TABLE employer(
                 name  TEXT,
                 age   TEXT,
                 email TEXT
             )

        e.g: create('employer', [('name', 'text'),
                                 ('age', 'INTEGER'),
                                 ('email', 'text')])

              CREATE TABLE employer(
                     name  TEXT,
                     age   INTEGER,
                     email TEXT
                 )


        Notes:
        ------

        . The fields are added to the table in the same order of the
           insertions.

        . A type is either a genral SQLite storage class (NULL, INTEGER,
          REAL, TEXT, and BLOB) or an affinity value. However, it is
          unsesetive case (e.g: Text <==> TEXT <==> text <==> tExT <==> etc.)

        . The default type of each field is "TEXT" except for "pk" which
          is "INTEGER".

        . When you try to create a table, you can omitted the 'text' value
          if the type of a column is a "TEXT" (as it is the default value).
          For example, this code will create a table called "employer":
                create('employer', [('name', 'text'),
                                 ('age', 'INTEGER'),
                                 ('email', 'text')])

          The following call will generate the same table as the previous
          example:
            create('employer', ['name', ('age', 'INTEGER'), 'email'])

        . If a tuple (a "column" values) in "fields" contains more than
          two values, only the first two items are selected (the 1st as
          the column's name and the 2nd as its type).

        . If you specifiy a "primary key", it should be included in "fields"
          and you should specity its type unless you satisfied with the default
          type which is "INTEGER".

        . If you pass a "primary key" VALUE to "pk" and this value (or column
          name) is not in "fields", this is still acceptable and an "INTEGER"
          primary key will be created, however, try to avoid this as it is
          more logically to include the "primary key" in "fields".

        . A "pk" is defined in the same way as in "fields" (either as a
          single str reperent the field's name or a tuple that contains the
          fields's name and its type).

        . If "auto" is not False and you specify a "pk", then "auto" will be
          ignored and the function will not generate an "id" field.


        Return:
        -------
         Return True if the table is created, and otherwise False.'''
        if table in self.database.TABLES:
            msg = f'"{table}" is already exists.'
            debug_mode(debug, msg)
            return False

        accepted = True
        specific = {"try to add an UNIQUE constraint to": uniques,
                     "specify the field to accept NULL values": null}
        for s in specific:
            for f in specific[s]:
                if f not in fields:
                    msg = f'Value Error: "{f}" is not defined, '
                    msg += f'\nYou {s}, "{f}" is an undefined field'
                    msg += f'\nDefined fields: {fields}'
                    debug_mode(debug, msg)
                    return False

        # List of SQLite affinity types
        affinity = [i.split('(')[0] for j in AFFINITY.values() for i in j]
        columns = {}  # List contains the table's fields and their types
        for field in fields:
            if not isinstance(field, (str, tuple, list)):
                msg = 'Type Error: unsupported type "'
                msg += f'{type(field)}" for {field}'
                debug_mode(debug, msg)
                return False

            if isinstance(field, str):  # Only the column's name is defined
                columns[field] = GENERAL[1] if pk and field == pk \
                                    else GENERAL[3]
            else:

                unsupported = None
                if not isinstance(field[0], str):
                    unsupported = field[0]
                elif not isinstance(field[1], str):
                    unsupported = field[1]

                if unsupported:
                    msg = 'Type Error: unsupported type '
                    msg += f'"{type(unsupported)}" for {unsupported} '
                    msg += f'in "{field}"'
                    msg += '\nOnly str values are acceptable'
                    debug_mode(debug, msg)
                    return False

                # Check if field type is a supported SQLite data type.
                field_type = field[1].upper()
                if field_type not in GENERAL:  # If not General Check AFFINITY

                    accepted = False
                    for ftype in affinity:
                        if field_type == ftype:
                            accepted = True
                            break

                    msg = f'Type Error: unsupported type "{field[1]}" for '
                    msg += f'"{field[0]}" in "{field}"\nYou try to create '
                    msg += 'a column with an unsupported data type.'
                    if not accepted:
                        debug_mode(debug, msg)
                        if debug:
                            msg = 'press (a) to display the supported types: '
                            display = input(msg)
                            if display.lower().startswith('a'):
                                print(f"\n\nGENERAL DATA TYPES: \n{GENERAL}")
                                print(f"\nAFFINITY DATA TYPES: \n{affinity}\n")
                        return False

                # Add the field's name and its type to columns
                columns[field[0]] = field_type

        # Generate SQLite Code
        codes = f"CREATE TABLE {table} ("

        # Check if the table has a primary key
        if auto and not pk:
            codes += "\n   id INTEGER NOT NULL PRIMARY KEY,"

        elif pk:
            if pk in columns:
                ptype = columns[pk]
                del columns[pk]
            else:
                ptype = GENERAL[1]
            pk_code = f"\n   {pk} {ptype} NOT NULL PRIMARY KEY,"
            del ptype

        # Complete the SQLite Code
        for field, atype in columns.items():

            # Check if field is pk
            if field == pk:  # Add the field in same order as in "fields"
                code += pk_code
                continue

            # Check if field accepts null values & Update the code
            is_null = "NULL" if field in null else "NOT NULL"
            codes += f"\n   {field} {atype} {is_null},"

        # Add UNIQUE CONTRAINTS
        if len(uniques) > 0:
            unique = '\n   UNIQUE('
            for field in uniques:
                unique += f"{field},"
            codes += unique[:-1] + '),'
        codes = codes[:-1] + '\n);'  # omitted the last comma
        #                             char & close the query
        breakpoint()
        # Execute the code
        operation = True
        with connect(self.database.current_db) as conn:
            cursor = conn.cursor()  # Create a cursor
            try:
                cursor.execute(codes)
                conn.commit()
            except (OperationalError, ) as error:
                if debug:
                    debug_mode(debug, error)
                operation = False
        return operation


    def drop(self, table: str) -> bool:
        '''Drop a table

           Drop the table if exists and return True,
           otherwise return False'''
        if table not in self.database.TABLES:
            return False
        return self.database.execute(f"DROP TABLE IF EXISTS {table}")


    def rename(self, old: str, new: str) -> bool:
        '''Rename an existed table

        old: str
        The current name

        new: str
        The new  name

        Return True if the process success, otherwise return False'''
        tables = self.database.TABLES
        if old not in tables or new in tables:
            return False
        return self.database.execute(f'''ALTER TABLE {old} RENAME TO {new};''')
