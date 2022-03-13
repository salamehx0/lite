# What is "Lite"?

Lite is a small and lighweight API for SQLite3.  
It facilitates the process of building and manipulating   
databases with SQLite3 and provides supports of some of     
the limitations of SQLite itself.   

While SQLite’s is good and has many great features, there  
are some things it currently does not implement. __*Lite*__ offers  
a set of solutions or implementations of some of these limitations.


# Features  

1. A simple and easy to use interface to create and manipulate  
   SQLite databases.  

2. Implementation for some of the most SQLite limitations.  


3. Support for diffrent __*ALTER TABLE*__ commands: 

     **SQLite** only supports the `RENAME TABLE` and `ADD COLUMN`  
     variants of the `ALTER TABLE` command. Other kinds of operations  
     such as `DROP COLUMN`, `ALTER COLUMN`, and `ADD CONSTRAINT` are  
     not implemented. __*Lite*__ supports and implements all the  
     previous commands except `ADD CONSTRAINT`.

4. Friendly Syntax
  e.g: 
      ```python
      from lite.lite import DB

      # Create a new table
      db = DB("TESTING.db")
      db.tables.create('employer', ['name', 'email', 'age'])


      # Create a new column
      db.columns.add('phone', ftype="INTEGER")
      ```
