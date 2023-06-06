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
    dag_id="populate_nyt_archive",
    schedule=datetime.timedelta(days=1),
    start_date=datetime.datetime.now(),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["nyt"]
)
def PopulateNYTArchive():
    # todo: Can operator read env file?
    create_nyt_archive_table = PostgresOperator(
        task_id="create_nyt_archive_table",
        postgres_conn_id="dev_db",
        sql="sql/create_nyt_archive.sql"
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

    def get_response(url):
        response = requests.get(url)
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            # todo: Throw exception on failed request.
            print("Request failed with status code:", response.status_code)


    # Year is YYYY; month is MM if between 10 and 12, else M.
    def get_archive(year, month):
        url = f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={NYT_API_KEY}"
        
        return get_response(url)["response"]["docs"]
    

    def get_previous_run():
        conn, cursor = get_connection()
        with conn:
            with cursor:
                query = """
                    SELECT 
                        MIN((article ->> 'pub_date')::date) AS pub_date
                    FROM nyt_archive
                """
                cursor.execute(query)
                result = cursor.fetchone()
                
        return result[0]


    def get_next_run():
        previous = get_previous_run()
        if previous:
            next = previous - datetime.timedelta(weeks=4)
        else:
            next = datetime.datetime.strptime("2022-12-01", "%Y-%m-%d")

        return next


    @task
    def get_data():
        date_tag = get_next_run()
        data = get_archive(date_tag.year, date_tag.month)

        return data


    @task
    def save_archive(archive):
        # Read data from workspace
        # Write to table
        conn, cursor = get_connection()

        with conn:
            with cursor:
                insert_query = "INSERT INTO nyt_archive (article) VALUES (%s)"
                for article in archive:
                    json_data = json.dumps(article)
                    cursor.execute(insert_query, (json_data,))

                conn.commit()


    read_archive = get_data()
    save_articles = save_archive(read_archive)

    create_nyt_archive_table >> read_archive >> save_articles

PopulateNYTArchive()
