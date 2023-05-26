# Install Airflow using the constraints file, which is determined based on the URL we pass:
export AIRFLOW_HOME=~/airflow

AIRFLOW_VERSION=2.6.1

# Extract the version of Python you have installed. If you're currently using Python 3.11 you may want to set this manually as noted above, Python 3.11 is not yet supported.
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.6.1 with python 3.7: https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.7.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"