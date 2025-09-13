"""
module v1.3.1
Data-Access-Layer and query builder for MySQL and SQLite
"""

import re
import csv
import operator
import warnings
import sqlite3
import mysql.connector
from . import flat

LAST_DATABASE_EXCEPTION: str = ''


class PDAException(Exception):
    """
    base exception class for this module
    """


"""
class to build the data description language for the connected database
"""
class DDL:
    __table: str = ''
    __primary_key: str = ''
    __fields: dict = {}
    __foreign_keys: dict = {}
    __unique: list = []
    __unique_constraint: list = []
    __indexes: dict = {}

    def __init__(self, table: str):
        """
        Initializes the DDL class with the table name.
        """
        self.__table = table
        self.__primary_key = ''
        self.__fields = {}
        self.__foreign_keys = {}
        self.__unique = []
        self.__unique_constraint = []
        self.__indexes = {}

    @staticmethod
    def table(table: str):
        """
        Static method to create a DDL instance.
        """
        return DDL(table)

    def integer(self, name: str, not_null: bool = False, auto_increment: bool = False, unique: bool = False, default: int = None):
        """
        Create an integer column.
        """
        self.__fields[name] = {'type': 'integer', 'not_null': not_null, 'auto_increment': auto_increment, 'unique': unique, 'default': default}
        return self

    def text(self, name: str, size: int = 255, not_null: bool = False, unique: bool = False, default: str = None):
        """
        Create a text column.
        """
        if default is not None:
            default = f'"{default}"'

        self.__fields[name] = {'type': 'text', 'size': size, 'not_null': not_null, 'auto_increment': False, 'unique': unique, 'default': default}
        return self

    def real(self, name: str, not_null: bool = False, default: float = None):
        """
        Create a real column.
        """
        self.__fields[name] = {'type': 'real', 'not_null': not_null, 'auto_increment': False, 'unique': False, 'default': default}
        return self

    def blob(self, name: str, not_null: bool = False):
        """
        Create a blob column.
        """
        self.__fields[name] = {'type': 'blob', 'not_null': not_null, 'auto_increment': False, 'unique': False, 'default': None}
        return self

    def datetime(self, name: str, not_null: bool = False, unique: bool = False, default: str = None):
        """
        Create a datetime column.
        """
        self.__fields[name] = {'type': 'datetime', 'not_null': not_null, 'auto_increment': False, 'unique': unique, 'default': default}
        return self

    def numeric(self, name: str, not_null: bool = False, default: any = None):
        """
        Create a numeric column.
        """
        self.__fields[name] = {'type': 'numeric', 'not_null': not_null, 'auto_increment': False, 'unique': False, 'default': default}
        return self

    def unique(self, field: str):
        """
        Create a single unique column constraint.
        """
        self.__unique.append(field)
        return self

    def unique_constraint(self, fields: str):
        """
        Create a multiple unique columns constraint.
        """
        self.__unique_constraint.append(fields)
        return self

    def primary_key(self, fields: str):
        """
        Create a primary key.
        """
        self.__primary_key = fields
        return self

    def foreign_key(self, fields: str, parent_table: str, primary_key: any):
        """
        Create a foreign key.
        """
        self.__foreign_keys[fields] = {'parent_table': parent_table, 'primary_key': primary_key}
        return self

    def index(self, fields: str, index_name: str = ''):
        """
        Create an index.
        """
        if not index_name:
            i = len(self.__indexes) + 1
            index_name = f"idx_{self.__table}_{i}"
        
        self.__indexes[index_name] = fields
        return self

    def create_sq3(self) -> str:
        """
        Build the ddl for sqlite database.
        """
        sql = f"create table {self.__table} ("

        for field, values in self.__fields.items():
            column_type = str(values['type']).upper()
            not_null = ' NOT NULL' if values['not_null'] is True else ''
            default = '' if values['default'] is None else f" DEFAULT {values['default']}"

            if values['auto_increment'] is True:
                self.primary_key(f"{field} AUTOINCREMENT")
                not_null = ''

            unique = ' UNIQUE' if values['unique'] is True and values['auto_increment'] is not True else ''
            sql += f"{field} {column_type}{not_null}{default}{unique}, "

        if self.__primary_key:
            sql += f"PRIMARY KEY({self.__primary_key}), "

        if self.__unique:
            for value in self.__unique:
                sql += f"UNIQUE({value}), "

        if self.__unique_constraint:
            for key, value in enumerate(self.__unique_constraint):
                constraint_name = f"{self.__table}_constraint{key}"
                sql += f"CONSTRAINT {constraint_name} UNIQUE ({value}), "

        if self.__foreign_keys:
            for key, value in self.__foreign_keys.items():
                parent_table = value['parent_table']
                parent_pk = value['primary_key']
                sql += f"FOREIGN KEY({key}) REFERENCES {parent_table} ({parent_pk}), "

        sql = sql[:-2] + ')'

        if self.__indexes:
            sql += ';\n'
            for index, fields in self.__indexes.items():
                sql += f"CREATE INDEX {index} ON {self.__table} ({fields});\n"
            sql = sql[:-1] # Remove the last newline

        return sql

    def create_msq(self) -> str:
        """
        Build the ddl for mysql database.
        """
        sql = f"create table {self.__table} ("

        for field, values in self.__fields.items():
            if values['type'] == 'integer':
                column_type = 'INT'
            elif values['type'] == 'text':
                size = values['size']
                column_type = f"VARCHAR({size})"
            elif values['type'] == 'real':
                column_type = 'FLOAT'
            elif values['type'] == 'blob':
                column_type = 'BLOB'
            elif values['type'] == 'datetime':
                column_type = 'DATETIME'
            else:
                column_type = 'INT'

            not_null = ' NOT NULL' if values['not_null'] is True else ''
            default = '' if values['default'] is None else f" DEFAULT {values['default']}"

            if values['auto_increment'] is True:
                auto_increment = ' AUTO_INCREMENT'
                if not self.__primary_key:
                    self.primary_key(field)
            else:
                auto_increment = ''

            if values['unique'] is True:
                self.unique(field)

            sql += f"{field} {column_type}{not_null}{default}{auto_increment}, "

        if self.__primary_key:
            sql += f"PRIMARY KEY({self.__primary_key}), "

        if self.__unique:
            for value in self.__unique:
                sql += f"UNIQUE({value}), "

        if self.__unique_constraint:
            for key, value in enumerate(self.__unique_constraint):
                constraint_name = f"{self.__table}_constraint{key}"
                sql += f"CONSTRAINT {constraint_name} UNIQUE ({value}), "

        if self.__foreign_keys:
            for key, value in self.__foreign_keys.items():
                parent_table = value['parent_table']
                parent_pk = value['primary_key']
                sql += f"FOREIGN KEY({key}) REFERENCES {parent_table} ({parent_pk}), "

        sql = sql[:-2] + ')'

        if self.__indexes:
            sql += ';\n'
            for index, fields in self.__indexes.items():
                sql += f"CREATE INDEX {index} ON {self.__table} ({fields});\n"
            sql = sql[:-1] # Remove the last newline

        return sql

    def create_flat(self) -> str:
        """
        Build the ddl for flatfile database.
        """
        sql = ''

        for field, values in self.__fields.items():
            required = 'REQUIRED' if values['not_null'] is True else ''
            column_type = str(values['type']).upper()
            auto_increment = ''

            if values['auto_increment'] is True:
                auto_increment = 'AUTOINCREMENT'
                if self.__primary_key and self.__primary_key != field:
                    raise Exception(f"primary key already declared for {self.__primary_key}")

                self.__primary_key = field

            if self.__primary_key and self.__primary_key == field:
                sql += f"{field} {column_type} PRIMARY_KEY {auto_increment}, "
            else:
                sql += f"{field} {column_type} {required}, "

        # Clean up the trailing comma and space and add a closing parenthesis if there are any fields.
        if sql:
            sql = sql[:-2] + ')'

        return sql


class Singleton(type):
    """
    metaclass in order to create a singleton
    """
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            instance = super().__call__(*args, **kwargs)
            cls.instances[cls] = instance

        return cls.instances[cls]


class Database(metaclass=Singleton):
    """
    class to deal with the different databases
    """
    __dbname = None
    __connection = None
    __dbtype = None

    def db_sq3(self, filename=''):
        """
        create a connection to a sqlite database
        """
        self.__dbname = filename
        self.__dbtype = 'SQ3'
        self.__connection = sqlite3.connect(self.__dbname)
        self.__connection.isolation_level = None  # we want autocommits
        self.__connection.row_factory = sqlite3.Row  # we want field value pairs
        self.__connection.execute("PRAGMA foreign_keys = 1")  # we want fk always checked
        return self

    def db_msq(self, dbhost: str = '', dbname: str = '', dbuser: str = '', dbpass: str = ''):
        """
        create a connection to a mysql database
        """
        self.__dbname = dbname
        self.__dbtype = 'MSQ'
        self.__connection = mysql.connector.connect(host=dbhost, database=dbname, user=dbuser, password=dbpass)
        return self

    def db_flat(self, path: str, name: str):
        """
        create a connection with a flatfile database
        """
        self.__dbname = name
        self.__dbtype = 'FLAT'
        self.__connection = flat.FlatDatabase(path, name).connect()
        return self

    def dbtype(self) -> str:
        """
        returns the type of database
        - SQ3: SQLite
        - MSQ> MySQL
        - FLAT: Flatfile
        """
        return self.__dbtype

    def connection(self):
        """
        returns the database connection
        """
        return self.__connection

    def name(self):
        """
        returns the database name
        """
        return self.__dbname

    def execute(self, stmt, params=None):
        """
        executes a database sql statement
        -
        - stmt: the sql statement
        - params: sql parameters
        - return: True when successfull, False when database exception
        """
        try:
            if params is None:
                self.__connection.execute(stmt)
            else:
                self.__connection.execute(stmt, params)

            return True
        except Exception as pdaex:  # pylint: disable=broad-except
            global LAST_DATABASE_EXCEPTION  # pylint: disable=global-statement
            LAST_DATABASE_EXCEPTION = str(pdaex)
            return False

    @staticmethod
    def isinitialized():
        """
        returns if the singleton is already initialized
        """
        return len(Singleton.instances) > 0

    @staticmethod
    def fetchone(cursor, stmt, params=None):
        """
        fetches one row from the database
        -
        - cursor: the database cursor
        - stmt: the sql statement
        - return: None when no results, dict when results found or False when database exception
        """
        try:
            if params is None:
                cursor.execute(stmt)
                result = cursor.fetchone()
            else:
                cursor.execute(stmt, params)
                result = cursor.fetchone()

            if result is None:
                return None

            return dict(result)

        except Exception as pdaex:  # pylint: disable=broad-except
            global LAST_DATABASE_EXCEPTION  # pylint: disable=global-statement
            LAST_DATABASE_EXCEPTION = str(pdaex)
            return False

    @staticmethod
    def fetchall(cursor, stmt, params=None):
        """
        fetches all rows from the database
        -
        - cursor: the database cursor
        - stmt: the sql statement
        - return: None when no results, list of dicts when results found or False when database exception
        """
        try:
            if params is None:
                cursor.execute(stmt)
                result = cursor.fetchall()
            else:
                cursor.execute(stmt, params)
                result = cursor.fetchall()

            if result is None:
                return None

            retvalue = []

            for i, data in enumerate(result):  # pylint: disable=unused-variable
                retvalue.append(dict(data))

            return retvalue

        except Exception as pdaex:  # pylint: disable=broad-except
            global LAST_DATABASE_EXCEPTION  # pylint: disable=global-statement
            LAST_DATABASE_EXCEPTION = str(pdaex)
            return False

    @staticmethod
    def exec(cursor, stmt, params=None):
        """
        executes a database sql statement
        -
        - cursor: the database cursor
        - stmt: the sql statement
        - params: sql parameters
        - return: True when successfull, False when database exception
        """
        try:
            if params is None:
                cursor.execute(stmt)
            else:
                cursor.execute(stmt, params)

            return True
        except Exception as pdaex:  # pylint: disable=broad-except
            global LAST_DATABASE_EXCEPTION  # pylint: disable=global-statement
            LAST_DATABASE_EXCEPTION = str(pdaex)
            return False


class Table():
    """
    dealing with a table in the database
    """
    _name: str = ''
    _ddl: DDL = None
    _where_pending = []

    def __init__(self, name: str = '', create_stmt: str = ''):
        """
        init class
        -
        - name: the name of the table
        - create_stmt: sql statment to create the table
        """

        if name:
            self._name = name

        self._ddl = self.ddl()
        self._where_pending = []

        if Database.isinitialized():
            dbtype = Database().dbtype()

            if dbtype == 'SQ3':
                self.instance = TableSQ3(self._name, create_stmt, self._ddl)
            elif dbtype == 'MSQ':
                self.instance = TableMSQ(self._name, create_stmt, self._ddl)
            elif dbtype == 'FLAT':
                self.instance = TableFlat(self._name, self._ddl)
            else:
                pass
        else:
            raise PDAException("no database connection found")

    @staticmethod
    def ddl():
        """
        method should be overwritten by using the DDL class
        """
        return ''

    @staticmethod
    def getsql(filename: str):
        """
        get a sql statement from a file
        """
        try:
            with open(filename, 'r', encoding="ascii") as file:
                return file.read()
        except OSError:
            return False

    @staticmethod
    def quote(stringvalue: str) -> str:
        """
        places a given string in single quotes
        -
        - stringvalue: the string
        - return: the string in quotes
        """
        return TableBaseClass.quote(stringvalue)

    def database(self) -> Database:
        """
        returns the tables database
        -
        """
        return self.instance.database()

    def tablename(self) -> str:
        """
        returns the name of the table
        """
        return self.instance.tablename/()

    def primarykey(self) -> str:
        """
        returns the tables primary key
        -
        """
        return self.instance.primarykey()

    def fieldlist(self) -> str:
        """
        returns a list of fields in a comma separated string
        -
        """
        return self.instance.fieldlist()

    def fields(self, field: str = '') -> dict:
        """
        returns one or all fields and their properties of the table
        -
        - field: name of a field in the table
        - return: field(s) properties
        """
        return self.instance.fields(field)

    def name(self) -> str:
        """
        returns the nanme of the table
        -
        """
        return self.instance.name()

    def create(self, sql: str):
        """
        creates the table in the database
        -
        """
        return self.instance.create(sql)

    def drop(self):
        """
        drops the table in the database
        -
        """
        return self.instance.drop()

    def insert(self, data: dict, empty_is_null: bool = True) -> bool:
        """
        insert a new row into the table
        -
        - data: fields and their values to be inserted
        - empty_is_null: should empty values be treated as NULL in the database
        """
        return self.instance.insert(data, empty_is_null)

    def delete(self, key) -> bool:
        """
        delete a row from the table
        -
        - key: a single value or a tuple with ordered! primary key values
        - return: True if successfull, otherwise False
        """
        return self.instance.delete(key)

    def deleteall(self) -> bool:
        """
        deletees ALL ! rows from the table.
        -
        - return: True if successfull, otherwise False
        """
        return self.instance.deleteall()

    def update(self, key, data: dict) -> bool:
        """
        updates a row from the table
        -
        - id: a single value or a tuple with ordered! primary key values
        - data: fields and their values to update
        - return: True if successfull, otherwise False
        """
        return self.instance.update(key, data)

    def updateall(self, data: dict) -> bool:
        """
        updates ALL ! rows from the table.
        -
        - data: fields and their values to update
        - return: True if successfull, otherwise False
        """
        return self.instance.updateall(data)

    def find(self, key):
        """
        finds a row in the table
        -
        - id: a single value or a tuple with ordered! primary key values
        - return: dict if successfull, None when nothing found, False when sql is shit
        """
        return self.instance.find(key)

    def where(self, field: str, value: any, compare: str = '=', conditional: str = 'and'):
        """
        chain function
        -
        - field: field name in the table
        - value: the value
        - compare: operator
        - conditional: operator
        """
        self._where_pending.append([field, value, compare, conditional])
        return self.instance.where(field, value, compare, conditional)

    def limit(self, limit: int = 0):
        """
        chain function
        -
        - limit: limit of the selection
        """
        return self.instance.limit(limit)

    def offset(self, offset: int = 0):
        """
        chain function
        -
        - offset: sets the selections offset
        """
        return self.instance.offset(offset)

    def addidentity(self, identify: bool = True):
        """
        chain function
        -
        - identify: add unique identifier field to the selection
        """
        return self.instance.addidentity(identify)

    def orderby(self, fields: str, direction: str = 'ASC'):
        """
        chain function
        -
        - fields: comma separated list of fields
        - direction: ASC or DESC
        """
        return self.instance.orderby(fields, direction)

    def count(self, select: str = '', prepared_params: tuple = ()) -> int:
        """
        counts the rows of the table
        -
        - select: the sql select statement
        - prepared_params: which values to pass to the statement
        """
        self._where_pending.clear()
        return self.instance.count(select, prepared_params)

    def findfirst(self, select: str = '', prepared_params: tuple = ()):
        """
        finds the first row in the table
        -
        - select: the sql select statement
        - prepared_params: which values to pass to the statement
        """
        return self.instance.findfirst(select, prepared_params)

    def findall(self, select: str = '', prepared_params: tuple = (), fetchone: bool = False):
        """
        finds all rows in the table
        -
        - select: the sql select statement
        - prepared_params: which values to pass to the statement
        - fetchone: fetch the first row of the result
        """
        self._where_pending.clear()
        return self.instance.findall(select, prepared_params, fetchone)

    def begintransaction(self):
        """
        starts a transaction
        -
        """
        return self.instance.begintransaction()

    def committransaction(self):
        """
        commits a transaction
        -
        """
        return self.instance.committransaction()

    def rollbacktransaction(self):
        """
        rolls a transaction back
        -
        """
        return self.instance.rollbacktransaction()

    def import_csv(self, **kwargs) -> bool:
        """
        imports data from a csv file into table
        -
        - filename: name of the file to store the data
        - fields: which fields should be exported
        - separator: default: ','
        - enclosure: default: '"'
        - escape: default: '\'
        - limit: default: 99999
        - offset: default: 0
        - quoting: default: QUOTE_ALL
        - on_insert_error: callable when insert failed
        """
        filename = kwargs.get('filename', f"{self._name}.csv")
        separator = kwargs.get('separator', ',')
        enclosure = kwargs.get('enclosure', '"')
        escape = kwargs.get('escape', '\\')
        limit = kwargs.get('limit', 99999)
        offset = kwargs.get('offset', 0)
        quoting = kwargs.get('quoting', csv.QUOTE_ALL)
        on_insert_error = kwargs.get('on_insert_error', None)

        linecount = 0
        fields = []

        with open(filename, mode='r', encoding='utf-8') as importfile:
            reader = csv.reader(importfile, delimiter=separator, quotechar=enclosure, escapechar=escape, quoting=quoting)

            for row in reader:
                if len(row) > 0 and limit > 0:
                    if linecount == 0:  # 1st line is the header
                        fields = row
                    elif linecount < offset:  # moving to offset
                        continue
                    else:
                        data = {}  # build data and insert row

                        try:
                            dataerror = False

                            for col, value in enumerate(row):
                                field = fields[col]
                                data[field] = value
                        except IndexError:
                            dataerror = True

                        if dataerror is True or self.insert(data) is False and on_insert_error is not None:  # on error call the callable
                            result = on_insert_error(linecount, data)

                            if result is False:  # callable suggested we should stop here
                                return False

                        limit -= 1

                    linecount += 1

        return True

    def export_csv(self, **kwargs) -> int:
        """
        exports table data to a csv file
        -
        - filename: name of the file to store the data
        - fields: which fields should be exported
        - separator: default: ','
        - enclosure: default: '"'
        - escape: default: '\'
        - limit: default: 1000
        - quoting: default: QUOTE_ALL
        """
        filename = kwargs.get('filename', f"{self._name}.csv")
        fields = kwargs.get('fields', self.fields())
        separator = kwargs.get('separator', ',')
        enclosure = kwargs.get('enclosure', '"')
        escape = kwargs.get('escape', '\\')
        limit = kwargs.get('limit', 1000)
        quoting = kwargs.get('quoting', csv.QUOTE_ALL)

        where_pending = self._where_pending
        rowcount = self.count()
        offset = 0
        lines = 0

        with open(filename, mode='w', encoding='utf-8') as exportfile:
            writer = csv.writer(exportfile, delimiter=separator, quotechar=enclosure, escapechar=escape, quoting=quoting)
            writer.writerow(fields)

            while offset < rowcount:
                if len(where_pending) > 0:
                    for values in where_pending:
                        param_field, param_value, param_compare, param__conditional = values
                        self.where(param_field, param_value, param_compare, param__conditional)

                data = self.limit(limit).offset(offset).findall()

                for data_row in data:
                    offset += 1
                    field_values = []

                    for field in fields:
                        field_values.append(data_row[field])

                    writer.writerow(field_values)
                    lines += 1

        return lines


class TableBaseClass:
    """
    implements properties and functions for a database table
    """
    _type: str = None
    _cursor = None
    _where_str: str = ''
    _where_arr: list = []
    _limit: int = 0
    _offset: int = 0
    _identify: bool = False
    _orderby: str = ''
    _db: Database = None
    _pk: dict = {}
    _name: str = ''
    _fields: dict = {}
    _pk_query: str = ''
    _meta_data: list = []
    _parameter_marker = '?'
    _ddl: DDL = None

    def __init__(self):
        """
        inits the class properties
        """
        self._type: str = None
        self._cursor = None
        self._where_str: str = ''
        self._where_arr: list = []
        self._limit: int = 0
        self._offset: int = 0
        self._identify: bool = False
        self._orderby: str = ''
        self._db: Database = None
        self._pk: dict = {}
        self._name: str = ''
        self._fields: dict = {}
        self._pk_query: str = ''
        self._meta_data: list = []
        self._parameter_marker = '?'
        self._ddl: DDL = None

    @staticmethod
    def quote(stringvalue: str) -> str:
        """
        returns the passed string in quotes
        """
        return "'" + stringvalue.replace("'", "''") + "'"

    def database(self) -> Database:
        """
        returns the database the table belongs to
        """
        return self._db

    def tablename(self) -> str:
        """
        returns the tablename
        """
        return self._name

    def primarykey(self) -> str:
        """
        returns the tables primary kez
        """
        return self._pk

    def fieldlist(self) -> str:
        """
        returns a comma separated list of the table column names
        """
        return ", ".join(self._fields.keys())

    def fields(self, field: str = ''):
        """
        returns a single field dictionary when a column names is passed or a dictionary
        of all field dictionaries when left blank
        """
        if not field:
            return self._fields

        return self._fields[field]

    def name(self) -> str:
        """
        returns the name of the table
        """
        return self._name

    def create(self, sql: str):
        """
        creates the table
        - raises exception
        """
        if not sql:
            raise PDAException("sql create statement is empty")

        if sql is None or self._name not in sql:
            raise PDAException("sql create statement invalid tablename")

        if Database.exec(self._cursor, sql) is False:
            raise PDAException(f"sql create table {self._name} statement failed")

        return self

    def drop(self):
        """
        drops the table
        - raises exception
        """
        sql = f"DROP TABLE IF EXISTS {self._name};"
        result = Database.exec(self._cursor, sql)

        if result is False:
            raise PDAException(f"table {self._name} cannot be dropped")

        return self

    def insert(self, data: dict, empty_is_null: bool = True) -> bool:
        """
        inserts a row into the table.
        - empty_is_null is used to decide if an empty string value '' should be inserted or left null
        - raises exception when using an unkown column.
        - returns true when the row cannot be inserted, otherwise false
        """
        cols = '('
        params = ' values ('
        vals = []

        for field, value in data.items():
            if value is None:
                continue

            if empty_is_null is True and isinstance(value, str) and not value:
                continue

            if field not in self._fields:
                raise PDAException(f"field {field} in table {self._name} not defined")

            cols += f"{field}, "
            params += f"{self._parameter_marker}, "
            vals.append(value)

        cols = cols[:-2] + ')'
        params = params[:-2] + ')'
        sql = f"insert into {self._name} {cols} {params}"
        return Database.exec(self._cursor, sql, tuple(vals))

    def delete(self, key) -> bool:
        """
        deletes a row from the table
        - returns true if successfull, else false
        """
        sql = f"delete from {self._name} where {self._pk_query}"

        if isinstance(key, dict):
            result = Database.exec(self._cursor, sql, tuple(key.values()))
        else:
            result = Database.exec(self._cursor, sql, (key, ))

        return result

    def deleteall(self):
        """
        deletes rows from the table
        """
        sql = f"DELETE FROM {self._name}"
        params = {}

        if self._where_str:
            sql += f" WHERE {self._where_str}"
            params = tuple(self._where_arr)
            self._where_str = ''
            self._where_arr.clear()

        result = Database.exec(self._cursor, sql, params)
        return result

    def update(self, key, data: dict) -> bool:
        """
        updates a single row
        - key: the primary key
        - data: column data to update
        """
        sql = f"update  {self._name} set "
        vals = []

        for field, value in data.items():
            if value is None:
                continue

            if field not in self._fields:
                raise PDAException(f"field {field} in table {self._name} not defined")

            vals.append(value)
            sql += f"{field}={self._parameter_marker}, "

        sql = sql[:-2] + f" where {self._pk_query}"

        if isinstance(key, dict):
            result = Database.exec(self._cursor, sql, tuple(data.values()) + tuple(key.values()))
        else:
            result = Database.exec(self._cursor, sql, tuple(data.values()) + (key, ))

        if result is False:
            raise PDAException(f"data cannot be updated in table {self._name}")

        return self._cursor.rowcount == 1

    def updateall(self, data: dict) -> bool:
        """
        updates all rows
        - data: column data to update
        """
        sql = f"UPDATE {self._name} SET "
        vals = []
        params = {}

        for field, value in data.items():
            if value is None:
                continue

            if field not in self._fields:
                raise PDAException(f"field {field} in table {self._name} not defined")

            vals.append(value)
            sql += f"{field}={self._parameter_marker}, "

        sql = sql[:-2]

        if self._where_str:
            sql += f" WHERE {self._where_str}"
            params = tuple(self._where_arr)
            self._where_str = ''
            self._where_arr.clear()

        result = Database.exec(self._cursor, sql, tuple(vals) + params)
        return result

    def find(self, key):
        """
        finds a single row in the table
        - key: the primary key of the table
        """
        sql = f"select * from {self._name} where {self._pk_query}"

        if isinstance(key, dict):
            result = Database.fetchone(self._cursor, sql, tuple(key.values()))
        else:
            result = Database.fetchone(self._cursor, sql, (key, ))

        return result

    def where(self, field: str, value: any, compare: str = '=', conditional: str = 'and'):
        """
        chain function: where
        - field: the fields name in the table
        - value: the fields value
        - compare: operator
        - conditional operator
        """
        if value is None:
            val = 'NULL'
        else:
            val = value

        if not self._where_str:
            self._where_str += f"{field} {compare} {self._parameter_marker}"
        else:
            self._where_str += f" {conditional} {field} {compare} {self._parameter_marker}"

        self._where_arr.append(val)
        return self

    def limit(self, limit: int = 0):
        """
        chain function: limit
        - limit: maxmimum rows to selection
        """
        self._limit = limit
        return self

    def offset(self, offset: int = 0):
        """
        chain function: offset
        - offset: the offset to use
        """
        self._offset = offset
        return self

    def addidentity(self, identify: bool = True):
        """
        chain function: addIdentity
        - addIdentity: adds an unique identifier to every selection
        """
        self._identify = identify
        return self

    def orderby(self, fields: str, direction: str = 'ASC'):
        """
        chain function: orderBy
        - fields: how to sort the resulting selection
        - direction: either ASC (ascending) or DESC (descending)
        """
        self._orderby = fields + ' ' + direction
        return self

    def count(self, select: str = '', prepared_params: tuple = ()) -> int:
        """
        chain function: count the selected rows
        - select: a select statement which will execute prior to a possible where statement
        - prepared_params: list of parameters for the select statement
        """
        if not select:
            sql = f"SELECT * FROM {self._name} "
        else:
            sql = select

        params = prepared_params

        if self._where_str:
            if len(prepared_params) > 0:
                sql += f" AND {self._where_str}"
                params = prepared_params + tuple(self._where_arr)
            else:
                sql += f" WHERE {self._where_str}"
                params = tuple(self._where_arr)

            self._where_str = ''
            self._where_arr.clear()

        sql = f"SELECT count(*) as count from ({sql}) as T"
        result = Database.fetchone(self._cursor, sql, params)

        if result is None:
            raise PDAException(f"count data from table {self._name} failed")

        if result is False:
            return 0

        return result['count']

    def findfirst(self, select: str = '', prepared_params: tuple = ()):
        """
        finds the first row in a selectc
        - select: the sql select statement
        - prepared_params: which values to pass to the statement
        """
        result = self.limit(1).findall(select, prepared_params, True)
        return result

    def findall(self, select: str = '', prepared_params: tuple = (), fetchone: bool = False):
        """
        finds all rows in the table
        -
        - select: the sql select statement
        - prepared_params: which values to pass to the statement
        - fetchone: fetch the first row of the result
        """
        pkey = next(iter(self._pk.values()))

        if self._identify is True:
            include_rowid = f", {pkey} as row_identifier "
        else:
            include_rowid = ""

        if not select:
            sql = f"SELECT * {include_rowid} FROM {self._name} "
        else:
            sql = re.sub('/\bfrom/i', include_rowid + ' from ', select)

        params = prepared_params

        if self._where_str:
            if len(prepared_params) > 0:
                sql += f" AND {self._where_str}"
                params = prepared_params + tuple(self._where_arr)
            else:
                sql += f" WHERE {self._where_str}"
                params = tuple(self._where_arr)

            self._where_str = ''
            self._where_arr.clear()

        if self._orderby:
            sql += f" ORDER BY {self._orderby}"
            self._orderby = ''

        if self._limit > 0:
            sql += f" LIMIT {self._limit}"
            self._limit = 0

            if self._offset > 0:
                sql += f" OFFSET {self._offset}"
                self._offset = 0

        if fetchone is True:
            result = Database.fetchone(self._cursor, sql, params)
        else:
            result = Database.fetchall(self._cursor, sql, params)

        if result is False:
            raise PDAException(f"findall data from table {self._name} failed")

        return result

    def begintransaction(self):
        """
        starts a transaction
        """
        Database.exec(self._cursor, "BEGIN")
        return self

    def committransaction(self):
        """
        commits a transaction
        """
        Database.exec(self._cursor, "COMMIT")
        return self

    def rollbacktransaction(self):
        """
        rolls back a transaction
        """
        Database.exec(self._cursor, "ROLLBACK")
        return self


class TableSQ3(TableBaseClass):
    """
    handles a sqlite table
    """

    def __init__(self, name: str, create_stmt: str, DDLdef=None, typedef: str = 'table'):
        """
        init class
        -
        - name: the name of the table
        - create_stmt: either a sql statment or a DDL callable
        - type: either 'table' or 'view'
        """
        super().__init__()
        self._type = typedef
        self._name = name
        self._ddl = DDLdef
        self._parameter_marker = '?'
        self._db = Database()

        name = self.quote(name)
        qtype = self.quote(typedef)
        self._cursor = self._db.connection().cursor()

        stmt = f"SELECT count(*) as count FROM sqlite_master WHERE type={qtype} AND name={name};"
        result = Database.fetchone(self._cursor, stmt)

        if result['count'] == 0:  # table does not exist
            if create_stmt is True:
                self.create(create_stmt)  # create it with passed create stmt
            else:
                self.create(self._ddl.create_sq3())

        stmt = f"PRAGMA table_info({name})"
        self._meta_data = Database.fetchall(self._cursor, stmt)

        if not self._meta_data:
            raise PDAException(f"cannot retrieve metadata from table {name}")

        for value in self._meta_data:  # building field dictionary from meta data
            if value['pk'] > 0:
                keynum = value['pk']
                name = value['name']
                self._pk[keynum] = name
                self._pk_query += f"{name}={self._parameter_marker} and "

            self._fields[value['name']] = {'type': value['type'], 'default': value['dflt_value'], 'required': value['notnull'] == 1}

        if not self._pk and typedef == 'table':  # a primary key for a table is mandatory
            raise PDAException(f"table {name} no primary key defined")

        if self._pk_query:
            self._pk_query = self._pk_query[:-5]

    def __del__(self):
        self._cursor.close()


class TableMSQ(TableBaseClass):
    """
    handles a mysql table
    """

    def __init__(self, name: str, create_stmt: str, DDLdef=None, typedef: str = 'table'):
        """
        init class
        -
        - name: the name of the table
        - create_stmt: either a sql statment or a DDL callable
        - type: either 'table' or 'view'
        """
        super().__init__()
        self._type = typedef
        self._name = name
        self._ddl = DDLdef
        self._parameter_marker = '%s'
        self._db = Database()

        self._cursor = self._db.connection().cursor(dictionary=True, buffered=True)
        stmt = f"SELECT 1 FROM {name};"
        result = Database.fetchone(self._cursor, stmt)

        if result is False:  # table does not exist
            if create_stmt is True:
                self.create(create_stmt)  # create it with passed create stmt
            else:
                self.create(self._ddl.create_msq())

        stmt = f"DESCRIBE {name}"
        self._meta_data = Database.fetchall(self._cursor, stmt)

        if not self._meta_data:
            raise PDAException(f"cannot retrieve metadata from table {name}")

        for key, value in enumerate(self._meta_data):  # building field dictionary from meta data
            if isinstance(value['Key'], bytearray) and value['Key'].upper() == b'PRI':
                keynum = key
                name = value['Field']
                self._pk[keynum] = name
                self._pk_query += f"{name}={self._parameter_marker} and "

            null_status = value['Null']

            if isinstance(null_status, bytearray):
                null_status = null_status.decode('utf-8')
            
            self._fields[value['Field']] = {'type': value['Type'], 'default': value['Default'], 'required': null_status.upper() == 'NO'}

        if not self._pk and typedef == 'table':  # a primary key for a table is mandatory
            raise PDAException(f"table {name} no primary key defined")

        if self._pk_query:
            self._pk_query = self._pk_query[:-5]

    def __del__(self):
        self._cursor.close()


class TableFlat(TableBaseClass):
    """
    handles a flatfile table
    """

    __table: flat.FlatTable

    def __init__(self, name: str, DDLdef=None, typedef: str = 'table'):
        super().__init__()
        self._type = typedef
        self._name = name
        self._ddl = DDLdef
        self._parameter_marker = ''
        self._db = Database().connection()
        self.__table = flat.FlatTable(self._db, self._name, self._ddl.create_flat())
        self._meta_data.clear()
        self._fields = self.__table.fields()
        self._pk[0] = self.__table.primary_key()  # we can have only a single field as primary key

        if not self._db.table_exists(self._name):
            self.create('')

    def create(self, sql: str):
        self._db.create_table(self._name)
        return self

    def drop(self):
        self._db.drop_table(self._name)
        return self

    def insert(self, data: dict, empty_is_null: bool = True) -> bool:
        try:
            return self.__table.insert(data)
        except flat.FlatTableException:
            return False
        except flat.FlatValidationException as pdaex:
            raise PDAException(pdaex.args) from pdaex

    def delete(self, key) -> bool:
        return self.__table.delete(key)

    def deleteall(self):
        for key in self.__table.findall(limit=self._limit, offset=self._offset, return_ids=True):
            self.delete(key)

        self._limit = 0
        self._offset = 0

    def update(self, key, data: dict) -> bool:
        try:
            result = self.__table.update(key, data)

            if result is False:
                return False

            return True
        except flat.FlatTableException:
            return False
        except flat.FlatValidationException as pdaex:
            raise PDAException(pdaex.args) from pdaex

    def updateall(self, data: dict) -> bool:
        for key in self.__table.findall(limit=self._limit, offset=self._offset, return_ids=True):
            self.update(key, data)

        self._limit = 0
        self._offset = 0
        return True

    def find(self, key):
        return self.__table.find(key)

    def where(self, field: str, value: any, compare: str = '=', conditional: str = 'and'):
        self.__table.where(field, value, compare, conditional)
        return self

    def addidentity(self, identify: bool = True):
        raise NotImplementedError()

    def count(self, select: str = '', prepared_params: tuple = ()) -> int:
        return self.__table.count()

    def findfirst(self, select: str = '', prepared_params: tuple = ()):
        result = self.findall()

        if len(result) > 0:
            return result[0]

        return False

    def findall(self, select: str = '', prepared_params: tuple = (), fetchone: bool = False):
        if self._orderby and (self._limit > 0 or self._offset > 0):
            # for computers memory sake, execution order of limit, offset and order_by is in reverse order.
            # therefore results are different from real databases, the order by is meant to be a sort of the
            # resuling rows
            warnings.warn('execution order of limit/offset and order by is reverse')

        result = self.__table.findall(limit=self._limit, offset=self._offset)

        self._limit = min(self._limit, 0)
        self._offset = min(self._offset, 0)

        if self._orderby:
            criteria = self._orderby.strip().replace('  ', '').split(" ")
            direction = criteria.pop()
            self._orderby = ''

            if direction.upper() == 'DESC':
                return sorted(result, key=operator.itemgetter(*criteria), reverse=True)

            return sorted(result, key=operator.itemgetter(*criteria))

        return result

    def begintransaction(self):
        raise NotImplementedError()

    def committransaction(self):
        raise NotImplementedError()

    def rollbacktransaction(self):
        raise NotImplementedError()
