AnySQL
-------
Lightweight, Thread-Safe, Version-Agnostic, SQL Client Implementation 
inspired by [Databases](https://github.com/encode/databases)

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
