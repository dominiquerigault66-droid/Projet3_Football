from dotenv import load_dotenv
import os, pymysql

load_dotenv()

print("DB_USER:", os.getenv('DB_USER'))
print("DB_PASS:", os.getenv('DB_PASS'))

conn = pymysql.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    port=int(os.getenv('DB_PORT', 3306))
)
cursor = conn.cursor()

with open('init_db.sql', 'r', encoding='utf-8') as f:
    sql = f.read()

for stmt in sql.split(';'):
    stmt = stmt.strip()
    if stmt:
        cursor.execute(stmt)

conn.commit()
print('✅ Tables criadas com sucesso!')
conn.close()