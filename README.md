# Lightweight and fast database access with query builder for MySQL and SQLite databases, as well as a Flatfile database implementation

The single module is trying to simplify the database access for your web application, preventing sql injections, making it easier to move from one database to another, offering a general interface to access the database.

### Features

- **Easy to use**: With a clear and intuitive API, you can get started quickly.

- **Secure:** Using this API significantly reduces the risk of SQL injections 

- **Scalable:**  You can start with the flat file database, move on to SQLite and then to MySQL without having to change your code

### Installation

`pip install python-data-access`

------

# Documentation

## Database

Connecting to a database need to be done only once in your application. After that the connection will be found.

### SQLite

```python
from easydb import pda

    db = pda.Database().db_sq3('/somewhere/dbtest.sqlite')
```

### MySql
```python
from easydb import pda

    host='localhost'
    database='dbtest'
    user='dbuser'
    password='dbpassword'
    db = pda.Database().db_msq(host, database, user, password)
```

### Flatfile

```python
from easydb import pda

    datapath='/somewhere'
    db = pda.Database().db_flat(datapath, 'dbtest.flat')

```

In cases where sqlite and mysql isnt available or you just want to store and retrieve some data, this might be a solution. Data are stored in the os filesystem.

## Tables

### Open a table

Opens a table and will raise an exception if the table doesnt exist:

```python
    products=pda.Table('Products')
```

### Open / create a table  

Opens a table and will create it if it doesnt exist:

```python
    stmt='CREATE TABLE Products\
          (ProductId INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\
           Description TEXT NOT NULL UNIQUE,\
           Price REAL, Min INTEGER,\
           Inactive INTEGER)'

    products=pda.Table('Products', stmt)
```

### Open / create a table using a model

```python
    class Products(pda.Table):
        _name: str = 'Products'

        def ddl(self):
            return pda.DDL(self._name) \
                .integer('ProductId', True, True, True) \
                .text('Description', 64, True, True) \
                .real('Price' ) \
                .integer('Min' ) \
                .integer('Inactive')
    
    products = Products()

class OrderDetails(pda.Table):
    _name: str = 'OrderDetails'

    def ddl(self):
        return pda.DDL(self._name) \
            .integer('OrderId', True) \
            .integer('Pos', True) \
            .integer('ProductId', True) \
            .integer('Qty', True) \
            .real('Price' ) \
            .text('Description') \
            .primary_key('OrderId, Pos') \
            .foreign_key('ProductId', 'Products', 'ProductId')

    order_details = OrderDetails()
```

### Drop a table

```python
    pda.Table('Products').drop()
```
or
```python
    Products().drop()
```

### Insert a row

```python
    result = products.insert({'Description':'A Product Description'}, 'Price': 25)
    result = order_details.insert({'OrderId': 2, 'Pos': 1, 'ProductId': 1, 'Qty': 10})
```

### Update row(s)

```python
    # set the column Min to zero where the primary key is 1
    result = products.update(1, {'Min': 500})

    # sets columns Min and Price to zero where column Inactive is 1 and column Description starts with A
    result = products.where('Inactive',1).where('Description', 'A%', 'like').updateall({'Min': 0, 'Price': 0})
```

### Delete row(s)

```python
    # delete the row with the primary key 1
    result = products.delete(1)

    # deletes all rows where the column Inactive is 1
    result = products.where('Inactive',1).deleteall()
```

### Find a row

```python
    result = products.find(1)
    result = order_details.find({'OrderId':2, 'Pos': 1})
```

### Counting rows

```python
    result = products.count()   # counts all rows
    result = products.where('Price', 100, '<').count()   # counts all rows where the price is less than 100
```

### Selecting rows from a table

```python
    # selects 5 rows
    result = products.limit(5).findall()
    
    # selects the 10 rows which are following after the 10th row
    result = products.limit(10).offset(10).findall()

    # select the first row
    result = products.findfirst()

    # select the first 10 rows which are inactive sorted by description 
    result = products.where('Inactive', 1).orderby('Description').limit(10).findall()

    # select all positions from an order
    result = order_details.where('OrderId', '2').findall()
```

### A more complex select

Lets assume, the following statement is stored in file named PRODUCTION.SQL

```python
    select
        Production.production_date as production_date,
        Products.description as description,
        Production.quantity as quantity,
        Production.details as details,
        Production.planned as planned,
        Production.produced as produced
    from 
        Production
    left join Products on 
        Production.product_id = Products.product_id
    order by
        production_date, description
```

You can load this statement and pass it on:

```python
        sql = products.getsql('products.sql')

        # select the first 10 rows from the above sql statement
        result = products.limit(10).findall(sql)

        # select all rows from the above sql statement where Desciption starts with A
        result = products.where('Description', 'A%', 'like').findall(sql)
```

### Import and export to csv file

```python
        products.import_csv('product_importdata.csv')
        products.export_csv('product_exportdata.csv')
```

### Running the Tests

The tests can be run individually i.e.:

```bash
python3 -m unittest discover tests test_pda_mysql.py
python3 -m unittest discover tests test_pda_sqlite.py
python3 -m unittest discover tests test_pda_flat.py
```

### Running Benchmark

```bash
python3 benchmarks.py -h
usage: benchmarks.py [-h] [-r ROWS]

optional arguments:
  -h, --help            show this help message and exit
  -r ROWS, --rows ROWS  set no. of rows to generate and process
```
The SQLite and Flatfile database will be created in the same folder in "./tests/data/[sqlite.db] and [flat.db]. In order to use Mysql, ypu will need to create a database. (host=localhost, dbname=db_test, dbuser=db_test, dbpass=db_password). If you are going to use something else, you will have to change the following line:

```python
bm = DBBenchmark('MSQ','Benchmarks for MySQL Database', dbname='db_test')
```

to your individual settings:

```python
bm = DBBenchmark('MSQ','Benchmarks for MySQL Database', host='xxx', dbname='xxx', dbuser='xxx', dbpass='xxx')
```

The output would be something like this:

```bash
Benchmarks for SQLite Database
   + insert 1000 rows in:  0.11565 secs
   + read 1000 rows in:  0.01676 secs
   + select and count rows: 100 counted in  0.00022 secs
   + delete 1000 rows in:  0.01205 secs
Benchmarks for MySQL Database
   + insert 1000 rows in:  0.70933 secs
   + read 1000 rows in:  0.2949 secs
   + select and count rows: 100 counted in  0.00092 secs
   + delete 1000 rows in:  0.22095 secs
Benchmarks for Flatfile Database
   + insert 1000 rows in:  0.37627 secs
   + read 1000 rows in:  0.0407 secs
   + select and count rows: 100 counted in  0.04923 secs
   + delete 1000 rows in:  0.01634 secs
```
