Code for [Airflow 3.0 Tutorial](https://www.startdataengineering.com/post/airflow-tutorial/)

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/josephmachado/airflow-tutorial)

## Local Setup 

**Prerequisites**

1. [Docker version >= 20.10.17](https://docs.docker.com/engine/install/) and [Docker compose v2 version >= v2.10.2](https://docs.docker.com/compose/#compose-v2-and-the-new-docker-compose-command).

Clone and start the container as shown below: 

```bash
git clone https://github.com/josephmachado/airflow-tutorial.git
cd airflow-tutorial
docker compose up -d --build
sleep 30 # wait 30 seconds for Airflow & Jupyter Notebook to start
```

Open Airflow at [http://localhost:8080](http://localhost:8080)

Open JupyterLab at [http://localhost:8888](http://localhost:8888)

Stop containers after you are done with `docker compose down -v`.

