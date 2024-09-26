import time
from easydb import pda
from random import randrange
from argparse import ArgumentParser

# ====================================================================
# V1.1.0 Benchmark databases
# ====================================================================


class TestModel(pda.Table):
    _name: str = 'BenchmarkTable'

    def ddl(self):
        return pda.DDL(self._name) \
            .integer('aId', True, True, True) \
            .text('aKey', 64, True, True) \
            .text('aString') \
            .integer('aInt')


class DBBenchmark():
    _dbtype: str = ''
    _caption: str = ''
    _db = None
    _table = None

    def __init__(self, dbtype: str, caption: str = '', **kwargs):
        datapath = kwargs.get('datapath', 'tests/data')
        host = kwargs.get('host', 'localhost')
        dbname = kwargs.get('dbname')
        dbuser = kwargs.get('dbuser', 'db_test')
        dbpass = kwargs.get('dbpass', 'db_password')
        self._dbtype = dbtype
        self._caption = caption

        try:
            if self._dbtype == 'SQ3':
                self._db = pda.Database().db_sq3(f"{datapath}/{dbname}")
            elif self._dbtype == 'MSQ':
                self._db = pda.Database().db_msq(host, dbname, dbuser, dbpass)
            elif self._dbtype == 'FLAT':
                self._db = pda.Database().db_flat(datapath, dbname)
            else:
                raise Exception(f"unknown database type {dbtype}")

            self._table = TestModel().drop()
            self._table = TestModel()
        except:
            self._db = None
            self._table = None

    def generateText(self, length: int = 20) -> str:
        characters = 'abcdefghijklmnopqrstuvwxyz'
        charactersLength = len(characters)
        randomString = ''

        for i in range(0, length):
            randomString += characters[randrange(0, charactersLength)]

        return randomString

    def timeDiff(self, start: float, caption: str = '') -> float:
        end = time.time()
        td = round((end - start), 5)

        if caption:
            print(caption, f"{td} secs")

        return td

    def executeBenchmarks(self, rows: int = 1000):
        if self._caption:
            print(self._caption)
            capt = '   + '
        else:
            capt = ''

        if self._db is None:
            print(f"{capt}not connected to a database !")
            return

        # --- WRITE ---

        print(f"{capt}insert {rows} rows in:", end="")
        timerStart = time.time()

        try:
            self._table.beginTransaction()  # wihout transaction processing time will increase significantly
        except:
            pass

        for i in range(1, rows):
            key = self.generateText(50)
            str = self.generateText(100)
            self._table.insert({'aKey': key, 'aString': str, 'aInt': i})

        try:
            self._table.commitTransaction()
        except:
            pass

        self.timeDiff(timerStart, ' ')

        # --- READ ---

        print(f"{capt}read {rows} rows in:", end="")
        timerStart = time.time()

        for i in range(1, rows):
            result = self._table.find(i)

        self.timeDiff(timerStart, ' ')

        # --- SELECT COUNT ---

        timerStart = time.time()
        print(f"{capt}select and count rows:", end="")
        result = self._table.where('aInt', 100, '>=').where('aInt', 200, '<').count()
        self.timeDiff(timerStart, f" {result} counted in ")

        # --- DELETE ---

        print(f"{capt}delete {rows} rows in:", end="")
        timerStart = time.time()

        try:
            self._table.begintransaction()  # wihout transaction processing time will increase significantly
        except:
            pass

        for i in range(1, rows):
            self._table.delete(i)

        try:
            self._table.committransaction()
        except:
            pass

        self.timeDiff(timerStart, ' ')


parser = ArgumentParser()
parser.add_argument("-r", "--rows", dest="rows",  default=1000, help="set no. of rows to generate and process")
args = parser.parse_args()

try:
    rows = int(args.rows)
except:
    rows = 1000
    print('invalid no. of rows, default used')

bm = DBBenchmark('SQ3', 'Benchmarks for SQLite Database', dbname='sqlite.db')
bm.executeBenchmarks(rows)
del bm

bm = DBBenchmark('MSQ', 'Benchmarks for MySQL Database', dbname='db_test')
bm.executeBenchmarks(rows)
del bm

bm = DBBenchmark('FLAT', 'Benchmarks for Flatfile Database', dbname='flat.db')
bm.executeBenchmarks(rows)
del bm
