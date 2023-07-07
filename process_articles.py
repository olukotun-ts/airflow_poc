import dotenv
import os
import psycopg2

dotenv.load_dotenv()

POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")

conn = psycopg2.connect(
    database=POSTGRES_DATABASE,
    host=POSTGRES_HOST,
    password=POSTGRES_PASSWORD, 
    port=POSTGRES_PORT,
    user=POSTGRES_USER
)

cursor = conn.cursor()

query = "SELECT headline FROM nyt_articles LIMIT 5;"

cursor.execute(query)
results = cursor.fetchall()

for result in results:
    print(result[0])

cursor.close()
conn.close()
