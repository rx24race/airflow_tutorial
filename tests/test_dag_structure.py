def test_dag_structure(dag_module):
    dag = dag_module.crypto_etl()

    assert dag.dag_id == "crypto_etl_real_world"
    assert dag.schedule == "@daily"
    assert set(dag.task_ids) == {
        "extract_and_validate.extract",
        "extract_and_validate.validate",
        "transform",
        "load",
    }
    assert dag.get_task("extract_and_validate.extract").downstream_task_ids == {
        "extract_and_validate.validate"
    }
    assert dag.get_task("extract_and_validate.validate").downstream_task_ids == {"transform"}
    assert dag.get_task("transform").downstream_task_ids == {"load"}
