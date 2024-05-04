import os
import shutil
import fcntl
import json
import re
from pathlib import Path


#====================================================================
# FLAT V1.0.0
#
# flatfile database
#====================================================================

#--------------------------------------------------------------------
class FlatException(Exception):
#--------------------------------------------------------------------
    pass

#--------------------------------------------------------------------
class FlatDBException(FlatException):
#--------------------------------------------------------------------
    pass

#--------------------------------------------------------------------
class FlatTableException(FlatException):
#--------------------------------------------------------------------
    pass

#--------------------------------------------------------------------
class FlatValidationException(FlatException):
#--------------------------------------------------------------------
    pass

#--------------------------------------------------------------------
class FlatDatabase():
#--------------------------------------------------------------------

    __path: str=''
    __name: str=''
    __fullpath: str=''
    __master: str=''
    __connected: bool=False


    def __init__(self, path: str, name: str):
        """
        init class
        -
        - path: the path to the database
        - name: the name of the database
        """ 
        self.__path = path
        self.__name = name
        self.__connected = False
        self.__fullpath = f"{self.__path}{os.sep}{self.__name}"
        self.__master = f"{self.__fullpath}{os.sep}.flat_database_master" 

        if not os.path.exists(self.__path):
            raise FlatDBException(f"path to flat database {self.__name} not found")

        if not self.databaseExists():
            self.createDatabase()


    def connect(self):
        """
        connects to the database
        -
        """ 
        if not os.path.exists(self.__master):
            raise FlatDBException(f"cannot connect to flat database {self.__name}")

        self.__connected = True
        return self


    def databaseExists(self)->bool:
        """
        checks if the database does exist
        -
        """ 
        return os.path.exists(self.__master)


    def createDatabase(self):
        """
        creates the database
        -
        """ 
        if self.__connected == True:
            raise FlatDBException(f"flat database {self.__name} already connected")

        if os.path.exists(self.__fullpath):
            raise FlatDBException(f"flat database {self.__name} already exists")

        Path(self.__fullpath).mkdir()
        Path(self.__master).mkdir()
        

    def fullPath(self)->str:
        """
        returns the path and database name as a path
        -
        """ 
        return self.__fullpath
    

    def createTable(self, name: str):
        """
        creates a table in the database
        -
        - name: the name of the table
        """ 
        if self.__connected == False:
            raise FlatDBException(f"not connected to database")

        location = f"{self.__fullpath}{os.sep}{name}{os.sep}"

        if os.path.exists(location):
            raise FlatDBException(f"table {name} does already exist")

        Path(location).mkdir()

        sequence = f"{self.__master}{os.sep}.sequence_{name}"

        with open(sequence, "w") as file:
            sequence = 0
            file.write(str(sequence))
            

    def dropTable(self, name: str):
        """
        drops a table in the database
        -
        - name: the name of the table
        """ 
        if self.__connected == False:
            raise FlatDBException(f"not connected to database")

        location = f"{self.__fullpath}{os.sep}{name}{os.sep}"

        if not os.path.exists(location):
            raise FlatDBException(f"table {name} does not exist")

        sequence = f"{self.__master}{os.sep}.sequence_{name}"

        if not os.path.isfile(sequence):
            raise FlatDBException(f"cannot drop table {name}")

        shutil.rmtree(location)
        os.remove(sequence)


    def tableExists(self, name: str)->bool:
        """
        checks if a table in the database exists
        -
        - name: the name of the table
        """ 
        if self.__connected == False:
            raise FlatDBException(f"not connected to database")

        location = f"{self.__fullpath}{os.sep}{name}{os.sep}"
        return os.path.exists(location)


    def sequence(self, name: str)->int:
        """
        increases the auto incremnt number of a table and returns it
        -
        - name: the name of the table
        """ 
        if self.__connected == False:
            raise FlatDBException(f"not connected to database")

        location = f"{self.__master}{os.sep}.sequence_{name}"
        sequence = 0

        with open(location, "r+") as file:
            fcntl.flock(file, fcntl.LOCK_EX)
            sequence = int(file.read(9)) + 1 
            file.seek(0)
            file.write(str(sequence))
            file.truncate()
            fcntl.flock(file, fcntl.LOCK_UN)
            
        return sequence


#--------------------------------------------------------------------
class FlatTable():
#--------------------------------------------------------------------
    __db: FlatDatabase=None
    __name: str=''
    __fullpath: str=''
    __pk: str=''
    __fields: dict={}
    __where_pending: list=[]

    def __init__(self, db:FlatDatabase, name: str, fields: str):
        """
        init class
        -
        - db: the flatfile database
        - name: the name of the database
        - fields: fields ddl
        """ 
        self.__db = db
        self.__name = name
        self.__fullpath = db.fullPath()+os.sep+self.__name+os.sep

        meta = fields.split(',')

        for value in meta:
            field_desc = value.strip().replace('  ', '').split(' ')
            field_name = field_desc[0]

            try:
                field_type = field_desc[1]
            except:
                field_type = 'TEXT'

            try:
                field_required = field_desc[2]

                if field_required.upper() == "PRIMARY_KEY":
                    self.__pk = field_name
                    field_required = True

                    if len(field_desc) > 2 and field_desc[3].upper() == "AUTOINCREMENT":
                        autoincrment = True
                    else:
                        autoincrment = False
                else:
                    autoincrment = False

                    if field_required.upper() == "REQUIRED":
                        field_required = True
                    else:
                        field_required = False
            except:
                autoincrment = False
                field_required = False

            self.__fields[field_name] = {'type': field_type, 'required': field_required, 'autoincrement': autoincrment}

        if not self.__pk:
            raise FlatTableException(f"no primary key defined for table {name}")
        

    def _where_pending(self, data: dict)->bool:
        operators = {
                '=': '==',
                '!=': '!=',
                '>': '>',
                '<': '<',
                '>=': '>=',
                '<=': '<=',
                'and': 'and',
                'AND': 'and',
                'or': 'or',
                'OR': 'or',
                'like': 'like',
                'LIKE':'like'
        }

        clause = ''
        result = True

        for condition in self.__where_pending:
            field = condition['field']
            op = operators[condition['op']]
            value = condition['value']
            type = condition['type']
            data_value = data[field]

            if op == 'like':
                pattern = '^'+value.replace('%', '.*')+'$'
                result = len(re.findall(pattern, data_value)) > 0
                clause += f"{result} == True"
            else:
                clause += f"{type} '{data_value}' {op} '{value}'"

        result = eval(clause)
        return result
            

    def where(self, field: str, value: str, op: str='=', conditional: str='and'):
        """
        adds a where condition to the select statement
        -
        - field: field name in the table
        - value: the value
        - op: operator
        - conditional: operator
        """ 
        if len(self.__where_pending) == 0:
            conditional = ''

        if field not in self.__fields:
            raise FlatTableException(f"field {field}  unknown")
            
        self.__where_pending.append({'type': conditional, 'field': field, 'op': op, 'value': value})
        return self


    def validateFields(self, data: dict):
        """
        checks if all given fields in the dict are valid
        -
        - data: the field - value dict
        """ 
        for key, value in data.items():
            if self.__fields.get(key) == None:
                raise FlatValidationException(f"field {key} is unknown")
            

        for field, properties in self.__fields.items():
            if field != self.__pk and properties['required'] == True  and not data.get(field, ''):
                raise FlatTableException(f"field {field}, a value is required")
        
        return self


    def primaryKey(self)->str:
        """
        returns the primary key of the table
        -
        """ 
        return self.__pk


    def fieldlist(self)->str:
        """
        returns comma separated string with all fields
        -
        """ 
        return ", ".join(self.__fields.keys())


    def fields(self, field: str=''):
        """
        returns one or all fields of the table
        -
        """ 
        if not field:
            return self.__fields
        else:
            return self.__fields[field]


    def name(self)->str:
        """
        returns the name of the table
        -
        """ 
        return self.__name
    

    def idExists(self, id)->bool:
        """
        checks if the primary key existss
        -
        """ 
        if type(id) == int:
            return os.path.isfile(self.__fullpath+str(id))
        else:
            return os.path.isfile(self.__fullpath+id)


    def insert(self, data: dict)->bool:
        """
        insert data into the table
        -
        - data: the field - value dict
        """ 
        self.validateFields(data)
        primary_key = data.get(self.__pk, '')

        if primary_key:
            if self.idExists(primary_key):
                raise FlatTableException(f"table {self.__name} duplicate primary key")
        else:
            if self.__fields[self.__pk]['autoincrement'] == True:
                try:
                    primary_key = str(self.__db.sequence(self.__name))
                    data[self.__pk] = primary_key
                except:
                    raise FlatTableException(f"table {self.__name} primary key is missing")
            else:
                raise FlatTableException(f"table {self.__name} primary key is missing")

            if self.idExists(primary_key):
                raise FlatTableException(f"table {self.__name} duplicate primary key")
        
        try:
            with open(self.__fullpath+primary_key, "w") as file: json.dump(data, file)
            return True
        except:
            return False


    def update(self, id, data: dict):
        """
        updates a row in the table
        -
        - id: the primary key
        - data: the field - value dict
        """ 
        if type(id) != str:
            key = str(id)
        else:
            key = id

        primary_key = data.get(self.__pk, '')

        if primary_key and primary_key != key: # check if the given id and the primary_key match
            raise FlatTableException(f"table {self.__name} primary key cannot be modified")
        
        if not self.idExists(key):
            return False
        
        try:
            with open(self.__fullpath+key, "r+") as file:
                fcntl.flock(file, fcntl.LOCK_EX)
                current_data = json.load(file)
                new_data = {**current_data, **data}
                self.validateFields(new_data)
                file.seek(0)
                json.dump(new_data, file)
                file.truncate()
                fcntl.flock(file, fcntl.LOCK_UN)

            return new_data
        except FlatValidationException as e:
            raise FlatValidationException(e.args)
        except:
            return False


    def find(self, id):
        """
        findes a row in the table
        -
        - id: the primary key
        """ 
        if type(id) != str:
            key = str(id)
        else:
            key = id

        try:
            with open(self.__fullpath+key, "r") as file:
                return json.load(file)
        except:
            return None
    
        
    def delete(self, id)->bool:
        """
        deletes a row in the table
        -
        - id: the primary key
        """ 
        if type(id) != str:
            key = str(id)
        else:
            key = id

        try:
            os.remove(self.__fullpath+key)
            return True
        except:
            return False
        

    def count(self)->int:
        """
        counts the rows in the table
        -
        - id: the primary key
        """ 
        counter = 0

        for dirpath, dirnames, filenames in os.walk(self.__fullpath[:-1]):
            for filename in filenames:
                if self.__where_pending:
                    data = self.find(filename)

                    if self._where_pending(data) == False:
                        continue

                counter += 1

            break

        self.__where_pending.clear()
        return counter


    def findAll(self, *, limit: int=0, offset: int=0, return_ids: bool=False):
        """
        finds rows in the table
        -
        - limit: sets limit of the selection
        - offset: sets the selection offset
        - return_ids: return a list of ids only
        """ 

        result = []
        offset_cnt = 0
        limit_cnt = 0

        for dirpath, dirnames, filenames in os.walk(self.__fullpath[:-1]):
            for filename in filenames:
                if offset > 0 and offset_cnt < offset:
                    offset_cnt +=1
                    continue

                data = self.find(filename)

                if self.__where_pending and self._where_pending(data) == False:
                    continue

                limit_cnt += 1

                if return_ids:
                    result.append(filename)
                else:
                    result.append(data)

                if limit > 0 and limit_cnt >= limit:
                    break

            break

        self.__where_pending.clear()
        return result

