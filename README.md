## Environment Variables
See [.env_template](.env_template) for the template. Fill in the values and save the file as `.env` in the root directory.

You don't want to commit this file to your repo, so ensure that your `.gitignore` file includes `.env`.


## Install Dependencies
- Postgres
- Airflow
- Python dependencies


## Running the App
The repo contains a [Docker Compose file](docker-compose.yaml). To run the app, enter the following command at your terminal:

```docker compose up```


### Included Services
CloudBeaver
- Frontend for managing Postgres database.
- Defaults to https://localhost:8081
- Default username: cbadmin
- Default password: cbadmin

To set up database connection in CloudBeaver, use the following default parameters.
- Host: postgres
- Port: 5432
- Database: airflow
- Username: airflow
- Password: airflow

Airflow UI
- Frontend for managing Airflow DAGs. 
- Defaults to https://localhost:8080
- Default username: airflow
- Default password: airflow

Postgres server
- Default database: airflow
- Default user: airflow
- Default password: airflow