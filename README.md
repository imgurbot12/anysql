AnySQL
-------
Lightweight, Thread-Safe, Version-Agnostic, SQL Client Implementation 
inspired by [Databases](https://github.com/encode/databases)

### Features

* **Lightweight** - no use of sqlalchemy or other massive frameworks
* **ThreadSafe**  - implements threadsafe features for fearless concurrent usage
* **Flexible**    - acts as a standard frontend for a wide variety of SQL backends
* **Powerful**    - simple API design with powerful utilities and quality-of-life features

### Installation

```
pip install anysql
```

### Security

It should be noted that anysql implements its own query parameterization to 
allow for greater API flexibility and performance, rather than rely on 
individual sql backends or relying on massive frameworks like sqlalchemy to 
handle query generation.

The existing parameterization has been thoroughly tested with 
[sqlmap](https://github.com/sqlmapproject/sqlmap), the world standard of 
sql pentesting-tools, to prevent and detect any possible sql-injection 
vulnerabilities.

The test-suite used is publically available within the source-code repo
within the [tests](https://github.com/imgurbot12/anysql/tree/master/tests)
folder.

### Example

```python
# Create a database instance, and connect to it.
from anysql import Database
database = Database('sqlite://:memory:')
database.connect()

# Create a table.
query = """CREATE TABLE HighScores (id INTEGER PRIMARY KEY, name VARCHAR(100), score INTEGER)"""
database.execute(query=query)

# Insert some data.
query = "INSERT INTO HighScores(name, score) VALUES (:name, :score)"
values = [
    {"name": "Daisy", "score": 92},
    {"name": "Neil", "score": 87},
    {"name": "Carol", "score": 43},
]
database.execute_many(query=query, values=values)

# Run a database query.
query = "SELECT * FROM HighScores"
rows = database.fetch_all(query=query)
print('High Scores:', rows)
```
