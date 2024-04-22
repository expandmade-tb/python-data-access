# Lightweight and fast database access with query builder for MySQL and SQLite databases.

The single module is trying to simplify the database access for your web application, preventing sql injections, making it easier to move from one database to another, offering a general interface to access the database.

## Database

Connecting to a database need to be done only once in your application. After that the connection will be found.

### SQLite

```python
    db = pdo.Database().DbSQ3('dbtest.db')
```

### MySql
```python
    host='localhost'
    database='dbtest'
    user='dbuser'
    password='dbpassword'
    db = pdo.Database().DbMSQ(host, database, user, password)
```

## Tables

### Open a table

Opens a table and will raise an exception if the table doesnt exist:

```python
    products=pdo.Table('Products')
```

### Open / create a table  

Opens a table and will create it if it doesnt exist:

```python
    stmt='CREATE TABLE Products\
          (ProductId INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\
           Description TEXT NOT NULL UNIQUE,\
           Price REAL, Min INTEGER,\
           Inactive INTEGER)'
           customer=pdo.Table('Products')

    products=pdo.Table('Products', stmt)
```

### Open / create a table using a model

```python
    class Products(pdo.Table):
        _name: str = 'Products'

        def DDL(self):
            return pdo.DDL(self._name) \
                .integer('ProductId', True, True, True) \
                .text('Description', 64, True, True) \
                .real('Price' ) \
                .integer('Min' ) \
                .integer('Inactive')
    
    products = Products()

class OrderDetails(pda.Table):
    _name: str = 'OrderDetails'

    def DDL(self):
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
    pdo.Table('Products').drop()
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
    result = products.where('Inactive',1).deleteAll()
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
    result = products.limit(5).findAll()
    
    # selects the 10 rows which are following after the 10th row
    result = products.limit(10).offset(10).findAll()

    # select the first row
    result = products.findFirst()

    # select the first 10 rows which are inactive sorted by description 
    result = products.where('Inactive', 1).orderBy('Description').limit(10).findall()

    # select all positions from an order
    result = order_details.where('OrderId', '2').findAll()
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
        sql = products.getSQL('products.sql')

        # select the first 10 rows from the above sql statement
        result = products.limit(10).findAll(sql)

        # select all rows from the above sql statement where Desciption starts with A
        result = products.where('Description', 'A%', 'like').findAll(sql)
```
       
### Import and export to csv file

```python
        products.import_csv('product_importdata.csv')
        products.export_csv('product_exportdata.csv')
```
