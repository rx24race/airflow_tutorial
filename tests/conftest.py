from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def dag_module():
    candidates = [
        Path(__file__).parents[1] / "airflow" / "dags" / "crypto_etl_real_world.py",
        Path(__file__).parents[1] / "dags" / "crypto_etl_real_world.py",
    ]
    dag_path = next(path for path in candidates if path.exists())
    spec = spec_from_file_location("crypto_etl_real_world", dag_path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
