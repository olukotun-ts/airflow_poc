import datetime
import dotenv
import json
import os
import psycopg2
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

dotenv.load_dotenv()

NYT_API_KEY = os.getenv("NYT_API_KEY")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")

@dag(
    dag_id="update_nyt_articles",
    schedule=datetime.timedelta(minutes=15),
    start_date=datetime.datetime.now(),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
    tags=["nyt"]
)
def UpdateNYTArticles():
    # todo: Can operator read env file?
    create_nyt_articles_table = PostgresOperator(
        task_id="create_nyt_articles_table",
        postgres_conn_id="dev_db",
        sql="sql/create_nyt_articles.sql"
    )

    # todo: Try PostgresOperator
    def get_connection():
        conn = psycopg2.connect(
            database = POSTGRES_DATABASE,
            host = POSTGRES_HOST,
            password = POSTGRES_PASSWORD,
            port = POSTGRES_PORT,
            user = POSTGRES_USER)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        return [conn, cursor]
    

    @task
    def read_nyt_archive():
        conn, cursor = get_connection()
        with conn:
            with cursor:
                query = """
                    SELECT 
                        REPLACE(article ->> 'uri', 'nyt://article/', '') AS id,
                        article -> 'headline' -> 'main' AS headline,
                        article -> 'abstract' AS abstract,
                        article -> 'lead_paragraph' AS lead_paragraph,
                        article -> 'byline' -> 'original' AS byline,
                        article -> 'type_of_material' AS type,
                        (article ->> 'pub_date')::timestamp AS pub_date,
                        article -> 'web_url' AS url
                    FROM nyt_archive
                    WHERE 
                        article ->> 'web_url' NOT IN (SELECT url FROM articles) AND
                        article ->> 'document_type' = 'article'
                    LIMIT 10;
                """
                cursor.execute(query)
                results = cursor.fetchall()
        
        return results


    @task
    def update_articles(articles):
        conn, cursor = get_connection()
        with conn:
            with cursor:
                insert_query = "INSERT INTO nyt_articles (id, headline, abstract, lead_paragraph, byline, type, pub_date, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                for article in articles:
                    cursor.execute(insert_query, article)
                conn.commit()


    read_nyt_archive_table = read_nyt_archive()
    update_articles_table = update_articles(read_nyt_archive_table)

    create_nyt_articles_table >> read_nyt_archive_table >> update_articles_table

UpdateNYTArticles()
