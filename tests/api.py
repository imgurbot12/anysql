"""
Testing API Server For SQL-Injection
"""
import sys
sys.path.insert(0, '../')

from anysql import Database
from anysql.escape import mogrify

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse
from fastapi_extras.session import SessionMiddleware

#** Variables **#

app = FastAPI()
app.add_middleware(SessionMiddleware)

db = Database('sqlite://:memory:')
# db = Database('mysql://testuser:password@192.168.0.37/test')
# db = Database('postgres://testuser@192.168.0.37/test')

#** Routes **#

@app.get('/')
def home(req: Request):
    """notify login status"""
    user = req.session.get('user')
    if user is not None:
        return f'You are logged in as {user!r}'
    return 'You are NOT logged in'

@app.get('/login')
def login(req: Request, username: str, password: str):
    """check login w/ username and password"""
    sql = 'SELECT id FROM Users WHERE username=? AND password=?'
    row = db.fetch_one(sql, (username, password))
    if row is not None:
        req.session['user'] = username
        return RedirectResponse('/', status_code=303)
    raise HTTPException(401, 
        f'Login Failure. Invalid Usernamee and Password: {username!r}')

@app.get('/logout')
def logout(req: Request):
    """logout if logged in"""
    if 'user' in req.session:
        del req.session['user']
        return 'You have logged out!'
    return 'You are NOT logged in!'

@app.on_event('startup')
def starup():
    print('db connecting')
    db.connect()
    db.execute('CREATE TABLE Users (id INTEGER PRIMARY KEY, username VARCHAR(25), password VARCHAR(50))')
    db.execute('INSERT INTO Users VALUES (?, ?, ?)', (1, 'admin', 'admin'))

@app.on_event('shutdown')
def shutdown():
    print('db disconnecting')
    db.disconnect()

#** Init **#
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app)
