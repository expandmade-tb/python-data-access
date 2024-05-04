import unittest
from lib import flat

class FlatTest(unittest.TestCase):
    datapath='tests/data'
    dbname='flat.db'
    tablename = 'Person'

    db = None
    Persons = None

    def step_000(self):
        print("flat test setup...")
        self.db = flat.FlatDatabase(self.datapath, self.dbname).connect()
        self.db.dropTable(self.tablename)

    def step_001(self):
        print("open / create table...")

        if not self.db.tableExists(self.tablename):
            self.db.createTable(self.tablename)

        ddl='PersonId integer primary_key autoincrement, first_name text, last_name text required, mail text'
        self.Persons = flat.FlatTable(self.db, self.tablename, ddl)

    def step_002(self):
        print("primary key...")
        result = self.Persons.primaryKey()
        self.assertEqual(result, 'PersonId')

    def step_003(self):
        print("fieldlist...")
        result = self.Persons.fieldlist()
        self.assertEqual(result, 'PersonId, first_name, last_name, mail')

    def step_004(self):
        print("insert with autoid...")
        result = self.Persons.insert({'first_name': 'John', 'last_name': 'Softwood'})
        self.assertEqual(result, True)

    def step_005(self):
        print("insert manual id...")
        result = self.Persons.insert({'PersonId': '2', 'first_name': 'Jim', 'last_name': 'Softwood'})
        self.assertEqual(result, True)

    def step_006(self):
        print("insert manual duplicate id...")
        with self.assertRaises(Exception): 
            result = self.Persons.insert({'PersonId': '2', 'first_name': 'Jane', 'last_name': 'Softwood'})

    def step_007(self):
        print("delete...")
        result = self.Persons.delete(2)
        self.assertEqual(result, True)

    def step_008(self):
        print("insert...")
        result = self.Persons.insert({'first_name': 'Jim', 'last_name': 'Softwood'})
        self.assertEqual(result, True)

    def step_009(self):
        print("insert...")
        result = self.Persons.insert({'first_name': 'Jane', 'last_name': 'Softwood'})
        self.assertEqual(result, True)

    def step_010(self):
        print("find...")
        result = self.Persons.find(3)
        self.assertIsNot(result, False)
        self.assertIsNotNone(result)
        self.assertEqual(result['first_name'], 'Jane')

    def step_011(self):
        print("update...")
        result = self.Persons.update(3, {'mail': 'jane.softwood@gmail.com'})
        self.assertIsNot(result, False)
        self.assertIsNotNone(result)
        self.assertEqual(result['mail'], 'jane.softwood@gmail.com')

    def step_012(self):
        print("count...")
        result = self.Persons.count()
        self.assertEqual(result, 3)


    def _steps(self):
        x = dir(self)
        for name in dir(self):
            if name.startswith("step"):
                yield name, getattr(self, name) 

    def test_steps(self):
        for name, step in self._steps():
            try:
                step()
            except Exception as e:
                self.fail("{} failed ({}: {})".format(step, type(e), e))
      
if __name__ == '__main__':
    unittest.main()
