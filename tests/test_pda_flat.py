import unittest
from pathlib import Path
from easydb import pda

# ====================================================================
# Unittest V1.1.0
#
# flatfile database
# ====================================================================


class TestModel(pda.Table):
    _name: str = 'Person'

    def ddl(self):
        return pda.DDL(self._name) \
            .integer('aId', True, True, True) \
            .text('aKey', 64, True, True) \
            .text('aString') \
            .integer('aInt')


class PdaTest(unittest.TestCase):
    # params for a db connection
    datapath = 'tests/data'
    dbname = 'flat.db'

    db = None
    table = None

    def step_000(self):
        print("Flat test setup...")
        Path(self.datapath).mkdir(exist_ok=True)

    def step_001(self):
        print("connect to flat database...")
        self.db = pda.Database().db_flat(self.datapath, self.dbname)

    def step_002(self):
        print("open table...")
        self.table = TestModel()

    def step_003(self):
        print("drop table...")
        self.table.drop()

    def step_004(self):
        print("create/open table...")
        self.table = TestModel()

    def step_005(self):
        print("tablename...", self.table.name())

    def step_006(self):
        print("primary key...")
        result = ",".join(self.table.primarykey().values())
        self.assertEqual('aId', result)

    def step_007(self):
        print("fieldList...")
        result = self.table.fieldlist()
        self.assertEqual('aId, aKey, aString, aInt', result)

    def step_008(self):
        print("insert unknown field...")

        with self.assertRaises(pda.PDAException):
            self.table.insert({'xKey': 'KeyVal', 'aString': 'StrVal', 'aInt': '1'})

    def step_009(self):
        print("insert valid data...")
        result = self.table.insert({'aKey': 'KeyVal1', 'aString': 'StrVal', 'aInt': '1'})
        self.assertEqual(result, True)

    def step_010(self):
        print("insert duplicate key...")
        result = self.table.insert({'aKey': 'KeyVal1', 'aString': 'StrVal', 'aInt': '1', 'aId': '1'})
        self.assertEqual(result, False)

    def step_011(self):
        print("insert more valid data...")
        result = self.table.insert({'aKey': 'KeyVal2', 'aString': 'StrVal', 'aInt': '1'})
        self.assertEqual(result, True)

    def step_012(self):
        print("insert empty value in a NOT NULL constraint...")
        result = self.table.insert({'aString': 'StrVal', 'aInt': '1'})
        self.assertEqual(result, False)

    def step_013(self):
        print("update 1st row...")
        result = self.table.update(1, {'aString': 'UpdatedStrVal'})
        self.assertEqual(result, True)

    def step_014(self):
        print("update invalid field...")

        with self.assertRaises(pda.PDAException):
            self.table.update(1, {'xString': 'UpdatedStrVal'})

    def step_015(self):
        print("update non existing id...")
        result = self.table.update(999, {'aString': 'UpdatedStrVal'})
        self.assertEqual(result, False)

    def step_016(self):
        print("find existing id...")
        result = self.table.find(1)
        self.assertIsNot(result, False)
        self.assertIsNotNone(result)
        self.assertEqual(result['aString'], 'UpdatedStrVal')

    def step_017(self):
        print("find non existing id...")
        result = self.table.find(999)
        self.assertEqual(result, None)

    def step_018(self):
        print("count rows...")
        result = self.table.count()
        self.assertEqual(result, 2)

    def step_019(self):
        print("count rows where equal...")
        result = self.table.where('aInt', 1).count()
        self.assertEqual(result, 2)

    def step_020(self):
        print("count rows where like...")
        result = self.table.where('aString', 'Updated%', 'like').count()
        self.assertEqual(result, 1)

    def step_021(self):
        print("findfirst order...")
        result = self.table.orderby('aKey').findfirst()
        self.assertIsNot(result, False)
        self.assertIsNotNone(result)
        self.assertEqual(result['aString'], 'UpdatedStrVal')

    def step_022(self):
        print("insert and delete...")
        result = self.table.insert({'aKey': 'KeyVal3', 'aString': 'StrVal', 'aInt': '1'})
        self.assertEqual(result, True)
        result = self.table.delete(3)
        self.assertEqual(result, True)

    def step_023(self):
        print("findall...")
        result = self.table.findall()
        self.assertIsNot(result, False)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)

    def step_024(self):
        print("findall limit...")
        result = self.table.limit(2).findall()
        self.assertIsNot(result, False)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)

    def step_025(self):
        print("findall where orderby...")
        result = self.table.where('aInt', 1).orderby('aKey').findall()
        self.assertEqual(result[0]['aId'], '1')

    def step_026(self):
        print("inject or 1=1...")
        result = self.table.where('aInt', '1 or 1=1').findall()
        self.assertEqual(len(result), 0)

    def step_027(self):
        print('inject or ""=""...')
        result = self.table.where('aId', '2 or ""=""').where('aString', 'StrVal or ""=""').findall()
        self.assertEqual(len(result), 0)

    def step_028(self):
        print('inject batched SQL...')
        result = self.table.where('aId', '1; drop table Person').findall()
        self.assertEqual(len(result), 0)

    def step_029(self):
        print('inject "--...')
        result = self.table.where('aId', '2 "--').where('aString', 'StrVal').findall()
        self.assertEqual(len(result), 0)

    def step_030(self):
        print("update all...")
        result = self.table.where('aInt', 1).updateall({'aInt': '99'})
        self.assertEqual(result, True)
        result = self.table.where('aInt', 99).count()
        self.assertEqual(result, 2)

    def step_031(self):
        print("delete all...")
        result = self.table.where('aInt', 99).deleteall()
        result = self.table.count()
        self.assertEqual(result, 0)

    def _steps(self):
        for name in dir(self):
            if name.startswith("step"):
                yield name, getattr(self, name)

    def test_steps(self):
        for name, step in self._steps():
            try:
                step()
            except Exception as e:
                print(f"\nLAST DATABASE EXCEPTION: {pda.LAST_DATABASE_EXCEPTION}")
                self.fail("{} failed ({}: {})".format(step, type(e), e))


if __name__ == '__main__':
    unittest.main()
