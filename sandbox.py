import sqlite3
import os

if 'test.db' in os.listdir():
    os.remove('test.db')

conn = sqlite3.connect("test.db")
c = conn.cursor()
c.execute('CREATE TABLE testing(' +
          'id INTEGER,' +
          'word VARCHAR)')
conn.commit()


conn.commit()

c.execute("SELECT * FROM testing WHERE id = 1")
result = c.fetchone()
print(type(result))

conn.close()