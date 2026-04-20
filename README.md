### 🚀 Airflow 3.0 Crypto ETL Pipeline (SCD Type 2)

This project demonstrates a production-grade Data Engineering pipeline using **Airflow 3.0**. It extracts real-time cryptocurrency data from the CoinGecko API, performs data quality validation, transforms the data using Pandas, and loads it into a PostgreSQL database using **Slowly Changing Dimension (SCD) Type 2** logic.

### ✨ Key Features
*   **Airflow 3.0 TaskFlow API**: Utilizes the latest Airflow features, including `TaskGroup` and functional task definitions.
*   **Real-time Data**: Integrates with the CoinGecko API for live market data.
*   **SCD Type 2 Loading**: Maintains a full history of price changes by tracking `effective_from` and `effective_to` dates.
*   **Data Quality**: Includes a validation layer to ensure data integrity before transformation.
*   **Modern Tooling**: Powered by `uv` for lightning-fast Python package management and Docker for containerization.

### 🛠 Tech Stack
*   **Orchestration**: Apache Airflow 3.0
*   **Data Processing**: Python, Pandas
*   **Database**: PostgreSQL
*   **Infrastructure**: Docker, Docker Compose
*   **Package Manager**: [uv](https://astral.sh/uv/)

### 📂 Project Structure
```text
.
├── airflow/
│   └── dags/
│       ├── sql/
│       │   ├── create_target_table.sql  # Database schema setup
│       │   └── merge_scd2.sql           # SCD Type 2 merge logic
│       └── crypto_etl_real_world.py      # Main DAG definition
├── Dockerfile                            # Custom Airflow image with uv
├── docker-compose.yml                    # Local infrastructure setup
└── README.md                             # You are here!
```

### 🚦 Getting Started

#### Prerequisites
*   [Docker](https://docs.docker.com/get-docker/) & Docker Compose
*   *Windows users*: It is highly recommended to use [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install).

#### Setup & Installation
1.  **Clone the repository**:
    ```bash
    git clone <your-repo-url>
    cd airflow_tutorial
    ```

2.  **Start the environment**:
    ```bash
    docker compose up -d --build
    ```

3.  **Access the interfaces**:
    *   **Airflow UI**: [http://localhost:8080](http://localhost:8080) (Default: No login required/Admin)
    *   **Jupyter Lab**: [http://localhost:8888](http://localhost:8888)

### 📊 The Pipeline (SCD Type 2 Logic)
The `crypto_etl_real_world` DAG follows these stages:

1.  **Extract & Validate**: Fetches top 10 coins by market cap and validates the presence of required fields.
2.  **Transform**:
    *   Adds analytics (e.g., `is_high_value` flag).
    *   Calculates market trends (Strong Bullish, Bearish, etc.).
    *   Handles data serialization for XCom compatibility.
3.  **Load (SCD2)**:
    *   Stages incoming data in a temporary table.
    *   Ends the validity of old records by updating `effective_to`.
    *   Inserts new records with a fresh `effective_from` timestamp.

### 🧼 Cleanup
To stop the services and remove volumes:
```bash
docker compose down -v
```

---

### 💡 Note on Connections
For the pipeline to run correctly, ensure you have a PostgreSQL connection named `airflow-3-db` configured in your Airflow instance.
