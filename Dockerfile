FROM python:3.13-bookworm

WORKDIR /home/airflow

# Install Java
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    openjdk-17-jdk \
    wget \
    make \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install uv
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"

# Airflow environment variables
ENV AIRFLOW_HOME=/home/airflow
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__FERNET_KEY=''
ENV AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true
ENV AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=3
ENV PYTHONDONTWRITEBYTECODE=1

# Install Airflow
ENV AIRFLOW_VERSION=3.1.7
ENV PYTHON_VERSION=3.13
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt"

RUN uv venv /home/airflow/.venv
RUN uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN uv pip install jupyterlab

# Start Airflow Server
COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

CMD ["/startup.sh"]
