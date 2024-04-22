import sqlite3
import mysql.connector
import re
import csv

#====================================================================
# PDA V1.0.0
#
# Data-Access-Layer and query builder for MySQL and SQLite 
#====================================================================

last_database_exception: str = ''

#--------------------------------------------------------------------
class PDOException(Exception):
#--------------------------------------------------------------------
    pass

#--------------------------------------------------------------------
class DDL():
#--------------------------------------------------------------------
    __table: str=''
    __primary_key: str=''
    __fields: dict={}
    __foreign_keys: dict={}
    __unique: list=[]
    __unique_constraint: list=[]


    def __init__(self, table: str):
        self.__table = table
        self.__primary_key: str=''
        self.__fields: dict={}
        self.__foreign_keys: dict={}
        self.__unique: list=[]
        self.__unique_constraint: list=[]


    def integer(self, name: str, not_null: bool=False, auto_increment: bool=False, unique: bool=False):
        self.__fields[name] = {'type': 'integer', 'not_null': not_null, 'auto_increment': auto_increment, 'unique': unique}
        return self


    def text(self, name: str, size: int=255, not_null: bool=False, unique: bool=False):
        self.__fields[name] = {'type': 'text', 'size': size, 'not_null': not_null, 'auto_increment': False, 'unique': unique}
        return self


    def real(self, name: str, not_null: bool=False):
        self.__fields[name] = {'type': 'real', 'not_null': not_null, 'auto_increment': False, 'unique': False}
        return self


    def blob(self, name: str, not_null: bool=False):
        self.__fields[name] = {'type': 'blob', 'not_null': not_null, 'auto_increment': False, 'unique': False}
        return self


    def unique(self, fields: str):
        self.__unique.append(fields)
        return self


    def unique_constraint(self, fields: str):
        self.__unique_constraint.append(fields)
        return self


    def primary_key(self, fields: str):
        self.__primary_key = fields
        return self
    
    def foreign_key(self, fields: str, parent_table: str, primary_key: any):
        self.__foreign_keys[fields] = {'parent_table': parent_table, 'primary_key': primary_key}
        return self
    

    def createSQ3(self)->str:
        sql = f"create table {self.__table} ("

        for field, values in self.__fields.items():
            type = values['type'].upper()
            not_null = ' NOT NULL' if values['not_null'] == True else ''
            auto_increment =  ' PRIMARY KEY AUTOINCREMENT' if values['auto_increment'] == True else ''
            unique =  ' UNIQUE' if values['unique'] == True and not auto_increment else ''
            sql += f"{field} {type}{not_null}{auto_increment}{unique}, "

        if self.__primary_key:
            sql += f"PRIMARY KEY({self.__primary_key}), "

        if self.__unique:
            for value in self.__unique:
                sql += f"UNIQUE({value}), "

        if self.__unique_constraint:
            for key, value in enumerate(self.__unique_constraint):
                constraint_name = value['parent_table']
                sql = f"CONSTRAINT {constraint_name} UNIQUE ({value}), "

        if self.__foreign_keys:
            for key, value in self.__foreign_keys.items():
                parent_table = value['parent_table']
                parent_pk = value['primary_key']
                sql += f"FOREIGN KEY({key}) REFERENCES {parent_table} ({parent_pk}), "

        sql = sql[:-2] + ')'
        return sql


    def createMSQ(self)->str:
        sql = f"create table {self.__table} ("

        for field, values in self.__fields.items():
            if values['type'] == 'integer':
                type = 'INT'
            elif values['type'] == 'text':
                size = values['size']
                type = f"VARCHAR({size})"
            elif values['type'] == 'real':
                type = 'FLOAT'
            elif values['type'] == 'blob':
                type = 'BLOB'
            else:
                type = 'INT'

            not_null = ' NOT NULL' if values['not_null'] == True else ''

            if values['auto_increment'] == True:
                auto_increment = ' AUTO_INCREMENT'
                self.primary_key(field)
            else:
                auto_increment = ''

            if values['unique'] == True:
                self.unique(field)

            sql += f"{field} {type}{not_null}{auto_increment}, "

        if self.__primary_key:
            sql += f"PRIMARY KEY({self.__primary_key}), "

        if self.__unique:
            for value in self.__unique:
                sql += f"UNIQUE({value}), "

        if self.__unique_constraint:
            for key, value in enumerate(self.__unique_constraint):
                constraint_name = value['parent_table']
                sql = f"CONSTRAINT {constraint_name} UNIQUE ({value}), "

        if self.__foreign_keys:
            for key, value in self.__foreign_keys:
                parent_table = value['parent_table']
                parent_pk = value['primary_key']
                sql += f"FOREIGN KEY({key}) REFERENCES {parent_table} ({parent_pk}), "

        sql = sql[:-2] + ')'
        return sql


#--------------------------------------------------------------------
class Singleton(type):
#--------------------------------------------------------------------
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance

        return cls._instances[cls]
    
#--------------------------------------------------------------------
class Database(metaclass=Singleton):
#--------------------------------------------------------------------
    __dbname = None
    __connection = None
    __dbtype = None

    def DbSQ3(self, filename=''):
        self.__dbname = filename
        self.__dbtype = 'SQ3'
        self.__connection = sqlite3.connect(self.__dbname)
        self.__connection.isolation_level = None # we want autocommits
        self.__connection.row_factory = sqlite3.Row # we want field value pairs    
        self.__connection.execute("PRAGMA foreign_keys = 1") # we want fk always checked
        return self


    def DbMSQ(self, dbhost: str='', dbname: str='', dbuser: str='', dbpass: str=''):
        self.__dbname = dbname
        self.__dbtype = 'MSQ'
        self.__connection = mysql.connector.connect(host=dbhost, database=dbname, user=dbuser, password=dbpass)
        return self
    

    def dbtype(self)->str:
        return self.__dbtype


    def connection(self):
        return self.__connection


    def name(self):
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
            if params == None:
                self.__connection.execute(stmt)
            else:
                self.__connection.execute(stmt, params)
                
            return True
        except Exception as e:
            global last_database_exception
            last_database_exception = str(e)
            return False


    @staticmethod
    def isInitialized():
        return len(Singleton._instances) > 0


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
            if params == None:
                cursor.execute(stmt)
                result = cursor.fetchone()
            else:
                cursor.execute(stmt, params)
                result = cursor.fetchone()

            if result is None:
                return None
            else:
                return dict(result)
        except Exception as e:
            global last_database_exception
            last_database_exception = str(e)
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
            if params == None:
                cursor.execute(stmt)
                result = cursor.fetchall()
            else:
                cursor.execute(stmt, params)
                result = cursor.fetchall()

            if result is None:
                return None
            else:
                for i in range(len(result)):
                    result[i] = dict(result[i])

                return result
        except Exception as e:
            global last_database_exception
            last_database_exception = str(e)
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
            if params == None:
                result = cursor.execute(stmt)
            else:
                result = cursor.execute(stmt, params)
                
            return True
        except Exception as e:
            global last_database_exception
            last_database_exception = str(e)
            return False


#--------------------------------------------------------------------
class Table():
#--------------------------------------------------------------------
    _name: str = ''
    _ddl: DDL = None
    _where_pending = []

    def __init__(self, name: str='', create_stmt: str=''):
        """
        init class
        -
        - name: the name of the table
        - create_stmt: sql statment to create the table
        """

        if name:
            self._name = name

        self._ddl = self.DDL()
        self._where_pending = []

        if Database.isInitialized():
            type = Database().dbtype()

            if type == 'SQ3':
                self.instance = TableSQ3(self._name, create_stmt, self._ddl)
            elif type == 'MSQ':
                self.instance = TableMSQ(self._name, create_stmt, self._ddl)
            else:
                pass
        else:
            raise PDOException(f"no database connection found")


    def DDL(self):
        return None


    @staticmethod
    def getSQL(filename: str):
        try:
            with open(filename, 'r') as file: return file.read()
        except:
            return False


    def quote(self, s: str)->str:
        """
        places a given string in single quotes
        -
        - s: the string
        - return: the string in quotes
        """ 
        return self.instance.quote(s)


    def database(self)->Database:
        """
        returns the tables database
        -
        """ 
        return self.instance.database()


    def tablename(self)->str:
        """
        returns the name of the table        -
        """ 
        return self.instance.tablename/()


    def primaryKey(self)->str:
        """
        returns the tables primary key
        -
        """ 
        return self.instance.primaryKey()


    def fieldList(self)->str:
        """
        returns a list of fields in a comma separated string
        -
        """ 
        return self.instance.fieldList()


    def fields(self, field: str='')->dict:
        """
        returns one or all fields and their properties of the table
        -
        - field: name of a field in the table
        - return: field(s) properties
        """ 
        return self.instance.fields(field)


    def name(self)->str:
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


    def insert(self, data: dict, empty_is_null: bool=True)->bool:
        """
        insert a new row into the table
        -
        - data: fields and their values to be inserted
        - empty_is_null: should empty values be treated as NULL in the database
        """ 
        return self.instance.insert(data, empty_is_null)


    def delete(self, id)->bool:
        """
        delete a row from the table
        -
        - id: a single value or a tuple with ordered! primary key values
        - return: True if successfull, otherwise False
        """ 
        return self.instance.delete(id)
            

    def deleteAll(self)->bool:
        """
        deletees ALL ! rows from the table. 
        -
        - return: True if successfull, otherwise False
        """ 
        return self.instance.deleteAll()
    

    def update(self, id, data: dict)->bool:
        """
        updates a row from the table
        -
        - id: a single value or a tuple with ordered! primary key values
        - data: fields and their values to update
        - return: True if successfull, otherwise False
        """ 
        return self.instance.update(id, data)
    

    def updateAll(self, data: dict)->bool:
        """
        updates ALL ! rows from the table. 
        -
        - data: fields and their values to update
        - return: True if successfull, otherwise False
        """ 
        return self.instance.updateAll(data)


    def find(self, id):
        """
        finds a row in the table
        -
        - id: a single value or a tuple with ordered! primary key values
        - return: dict if successfull, None when nothing found, False when sql is shit
        """ 
        return self.instance.find(id)
    

    def where(self, field: str, value: any, compare: str='=', conditional: str='and'):
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
    

    def limit(self, limit: int=0):
        """
        chain function
        -
        - limit: limit of the selection
        """ 
        return self.instance.limit(limit)
    

    def offset(self, offset: int=0):
        """
        chain function
        -
        - offset: sets the selections offset
        """ 
        return self.instance.offset(offset)
    

    def addIdentity(self, identify: bool = True):
        """
        chain function
        -
        - identify: add unique identifier field to the selection 
        """ 
        return self.instance.addIdentity(identify)
    

    def orderBy(self, fields: str, direction: str='ASC'):
        """
        chain function
        -
        - fields: comma separated list of fields
        - direction: ASC or DESC 
        """ 
        return self.instance.orderBy(fields, direction)
    

    def count(self, select: str = '', prepared_params: tuple=() )->int:
        """
        counts the rows of the table
        -
        - select: the sql select statement
        - prepared_params: which values to pass to the statement
        """
        self._where_pending.clear()
        return self.instance.count(select, prepared_params)


    def findFirst(self, select: str = '', prepared_params: tuple=() ):
        """
        finds the first row in the table
        -
        - select: the sql select statement
        - prepared_params: which values to pass to the statement
        """ 
        return self.instance.findFirst(select, prepared_params)


    def findAll(self, select: str = '', prepared_params: tuple=(), fetchone: bool=False ):
        """
        finds all rows in the table
        -
        - select: the sql select statement
        - prepared_params: which values to pass to the statement
        - fetchone: fetch the first row of the result
        """ 
        self._where_pending.clear()
        return self.instance.findAll(select, prepared_params)


    def beginTransaction(self):
        """
        starts a transaction
        -
        """
        return self.instance.beginTransaction()


    def commitTransaction(self):
        """
        commits a transaction
        -
        """
        return self.instance.commitTransaction()


    def rollbackTransaction(self):
        """
        rolls a transaction back
        -
        """
        return self.instance.rollbackTransaction()
    

    def import_csv(self, *args, **kwargs)->bool:
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

        with open(filename, mode='r') as importfile:
            reader = csv.reader(importfile, delimiter=separator, quotechar=enclosure, escapechar=escape, quoting=quoting)

            for row in reader:
                if len(row) > 0 and limit > 0:
                    if linecount == 0: # 1st line is the header
                        fields = row
                    elif linecount < offset: # moving to offset
                        continue
                    else:
                        data = {} # build data and insert row

                        try:
                            dataerror = False
     
                            for col, value in enumerate(row):
                                field = fields[col]
                                data[field] = value
                        except:
                            dataerror = True

                        if dataerror == True or self.insert(data) == False and on_insert_error is not None: # on error call the callable
                            result = on_insert_error(linecount, data)

                            if result == False: # callable suggested we should stop here
                                return False

                        limit -= 1

                    linecount += 1

        return True

    def export_csv(self, *args, **kwargs)->int:
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
        
        with open(filename, mode='w') as exportfile:
            writer = csv.writer(exportfile, delimiter=separator, quotechar=enclosure, escapechar=escape, quoting=quoting)
            writer.writerow(fields)

            while offset < rowcount:
                if len(where_pending) > 0:
                    for values in where_pending:
                        param_field, param_value, param_compare, param__conditional = values
                        self.where(param_field, param_value, param_compare, param__conditional)

                data = self.limit(limit).offset(offset).findAll()

                for data_row in data:
                    offset += 1
                    field_values = []

                    for field in fields:
                        field_values.append(data_row[field])

                    writer.writerow(field_values)
                    lines += 1

        return lines


#--------------------------------------------------------------------
class TableBaseClass:
#--------------------------------------------------------------------
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
        print('specifig implementation missing')
        return self

    def quote(self, s: str)->str:
        return "'" + s.replace("'", "''") + "'" 
    

    def database(self)->Database:
        return self._db


    def tablename(self)->str:
        return self._name


    def primaryKey(self)->str:
        return self._pk


    def fieldList(self)->str:
        return ", ".join(self._fields.keys())


    def fields(self, field: str=''):
        if not field:
            return self._fields
        else:
            return self._fields[field]


    def name(self)->str:
        return self._name


    def create(self, sql: str):
        if not sql:
            raise PDOException("sql create statement is empty") 
        
        if sql == None or self._name not in sql:
            raise PDOException("sql create statement invalid tablename") 

        if Database.exec(self._cursor, sql) == False:
            raise PDOException(f"sql create table {self._name} statement failed") 
        
        return self


    def drop(self):
        sql = f"DROP TABLE IF EXISTS {self._name};"
        result = Database.exec(self._cursor, sql) 

        if result == False:
            raise PDOException(f"table {self._name} cannot be dropped")
        
        return self
    

    def insert(self, data: dict, empty_is_null: bool=True)->bool:
        cols = '('
        params = ' values ('
        vals = []

        for field, value in data.items():
            if value is None:
                continue

            if empty_is_null == True and type(value) == str and not value:
                continue

            if field not in self._fields:
                raise PDOException(f"field {field} in table {self._name} not defined")
            
            cols += f"{field}, "
            params += f"{self._parameter_marker}, "
            vals.append(value)
            
        cols = cols[:-2] + ')'
        params = params[:-2] + ')'
        sql = f"insert into {self._name} {cols} {params}"
        return Database.exec(self._cursor, sql, tuple(vals)) 


    def delete(self, id)->bool:
        sql = f"delete from {self._name} where {self._pk_query}"
        
        if type(id) == dict:
            result = Database.exec(self._cursor, sql, tuple(id.values())) 
        else:
            result = Database.exec(self._cursor, sql, (id, )) 
        
        return result
    
    
    def deleteAll(self):
        sql = f"DELETE FROM {self._name}"
        params = {}

        if self._where_str:
            sql += f" WHERE {self._where_str}"
            params = tuple(self._where_arr)
            self._where_str = ''
            self._where_arr.clear()

        result = Database.exec(self._cursor, sql, params)
        return result

        
    def update(self, id, data: dict)->bool:
        sql = f"update  {self._name} set "
        vals = []

        for field, value in data.items():
            if value is None:
                continue

            if field not in self._fields:
                raise PDOException(f"field {field} in table {self._name} not defined")
            
            vals.append(value)
            sql += f"{field}={self._parameter_marker}, "

        sql = sql[:-2] + f" where {self._pk_query}"  

        if type(id) == dict:
            result = Database.exec(self._cursor, sql, tuple(data.values() ) + tuple(id.values())) 
        else:
            result = Database.exec(self._cursor, sql, tuple(data.values()) + (id, )) 
        
        if result == False:
            raise PDOException(f"data cannot be updated in table {self._name}")

        if self._cursor.rowcount == 1:
            return True
        else:
            return False


    def updateAll(self, data: dict)->bool:
        sql = f"UPDATE {self._name} SET "
        vals = []
        params = {}

        for field, value in data.items():
            if value is None:
                continue

            if field not in self._fields:
                raise PDOException(f"field {field} in table {self._name} not defined")
            
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


    def find(self, id):
        sql = f"select * from {self._name} where {self._pk_query}"

        if type(id) == dict:
            result = Database.fetchone(self._cursor, sql, tuple(id.values()))
        else:
            result = Database.fetchone(self._cursor, sql, (id, ))

        return result
    

    def where(self, field: str, value: any, compare: str='=', conditional: str='and'):
        if value is None:
            val = 'NULL'
        else:
            val = value

        if not self._where_str:
            self._where_str += f"{field} {compare} {self._parameter_marker}"
        else:
            self._where_str +=  f" {conditional} {field} {compare} {self._parameter_marker}"

        self._where_arr.append(val)
        return self


    def limit(self, limit: int=0):
        self._limit = limit
        return self


    def offset(self, offset: int=0):
        self._offset = offset
        return self


    def addIdentity(self, identify: bool=True):
        self._identify = identify
        return self 


    def orderBy(self, fields: str, direction: str='ASC'):
        self._orderby = fields + ' ' + direction
        return self
        

    def count(self, select: str='', prepared_params: tuple=() )->int:
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
            raise PDOException(f"count data from table {self._name} failed")
        
        if result == False:
            return 0
        else:
            return result['count']

              
    def findFirst(self, select: str='', prepared_params: tuple=()):
        result = self.limit(1).findAll(select, prepared_params, True)
        return result


    def findAll(self, select: str='', prepared_params: tuple=(), fetchone: bool=False):
        pk = next(iter(self._pk.values()))

        if self._identify == True:
            include_rowid = f", {pk} as row_identifier "
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

        if fetchone == True:
            result = Database.fetchone(self._cursor, sql, params)
        else:
            result = Database.fetchall(self._cursor, sql, params)

        if result == False:
            raise PDOException(f"findAll data from table {self._name} failed")

        return result


    def beginTransaction(self):
        result = Database.exec(self._cursor, "BEGIN") 
        return self


    def commitTransaction(self):
        result = Database.exec(self._cursor, "COMMIT") 
        return self


    def rollbackTransaction(self):
        result = Database.exec(self._cursor, "ROLLBACK") 
        return self

#--------------------------------------------------------------------
class TableSQ3(TableBaseClass):
#--------------------------------------------------------------------
    def __init__(self, name: str, create_stmt: str, DDL=None, type: str='table' ):
        """
        init class
        -
        - name: the name of the table
        - create_stmt: either a sql statment or a DDL callable
        - type: either 'table' or 'view'
        """ 
        self._type = type
        self._name = name
        self._ddl = DDL
        self._parameter_marker = '?'
        self._db = Database()
 
        name = self.quote(name)
        type = self.quote(type)
        self._cursor = self._db.connection().cursor()

        stmt = f"SELECT count(*) as count FROM sqlite_master WHERE type={type} AND name={name};"
        result = Database.fetchone(self._cursor, stmt)

        if result['count'] == 0: # table does not exist
            if create_stmt == True:
                self.create(create_stmt) # create it with passed create stmt
            else:
                self.create(self._ddl.createSQ3())

        stmt = f"PRAGMA table_info({name})"
        self._meta_data = Database.fetchall(self._cursor, stmt)

        if self._meta_data is None or False:
              raise PDOException(f"cannot retrieve metadata from table {name}")
        
        for value in self._meta_data: # building field dictionary from meta data
            if value['pk'] > 0:
                keynum = value['pk']
                name = value['name']
                self._pk[keynum] = name
                self._pk_query += f"{name}={self._parameter_marker} and "

            self._fields[value['name']] = {'type': value['type'], 'default': value['dflt_value'], 'required': True if value['notnull'] == 1 else False}

        if not self._pk and type == 'table': # a primary key for a table is mandatory
              raise PDOException(f"table {name} no primary key defined")

        if self._pk_query:
            self._pk_query = self._pk_query[:-5]


    def __del__(self): 
        self._cursor.close()


#--------------------------------------------------------------------
class TableMSQ(TableBaseClass):
#--------------------------------------------------------------------
    def __init__(self, name: str, create_stmt: str, DDL=None, type: str='table' ):
        """
        init class
        -
        - name: the name of the table
        - create_stmt: either a sql statment or a DDL callable
        - type: either 'table' or 'view'
        """ 
        self._type = type
        self._name = name
        self._ddl = DDL
        self._parameter_marker = '%s'
        self._db = Database()
 
        self._cursor = self._db.connection().cursor(dictionary=True, buffered=True)
        stmt = f"SELECT 1 FROM {name};"
        result = Database.fetchone(self._cursor, stmt)

        if result == False: # table does not exist
            if create_stmt == True:
                self.create(create_stmt) # create it with passed create stmt
            else:
                self.create(self._ddl.createMSQ())

        stmt = f"DESCRIBE {name}" 
        self._meta_data = Database.fetchall(self._cursor, stmt)

        if self._meta_data is None or False:
              raise PDOException(f"cannot retrieve metadata from table {name}")
        
        for key, value in enumerate(self._meta_data): # building field dictionary from meta data
            if value['Key'] == 'PRI':
                keynum = key
                name = value['Field']
                self._pk[keynum] = name
                self._pk_query += f"{name}={self._parameter_marker} and "

            self._fields[value['Field']] = {'type': value['Type'], 'default': value['Default'], 'required': True if value['Null'] == 'NO' else False}

        if not self._pk and type == 'table': # a primary key for a table is mandatory
              raise PDOException(f"table {name} no primary key defined")

        if self._pk_query:
            self._pk_query = self._pk_query[:-5]


    def __del__(self): 
        self._cursor.close()