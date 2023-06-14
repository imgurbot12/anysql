Testing
--------

AnySQL uses this simple http server to allow sqlmap an API in order to fuzz 
and validate AnySQL's builtin query parameterization security. 

It's designed to be as simple as possible while also implementing something
that could easily be a real-world implementation.

### Prerequisites

1. Install anysql and a compatible backend driver. eg: `sqlite/pymysql/psycopg2`
2. Install requirements.txt with `pip install -r requirements.txt`
3. Install or Download [SQLMap](https://github.com/sqlmapproject/sqlmap)

### How To Run

1. Run the HTTP API with `python3 api.py`
2. In a separate terminal run `run-tests.sh` with `bash run-tests.sh`
