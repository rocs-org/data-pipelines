from mlflow import search_experiments, log_metric, log_param, set_tracking_uri, set_experiment

def test_mlflow_works_returns_logs():
    set_tracking_uri("http://localhost:8800")
    set_experiment("my-experiment")
    # Log a parameter
    log_param("param1", 5)
    # Log a metric
    log_metric("foo", 10)
    experiments = search_experiments(filter_string="name = 'my-experiment'")
    assert [e.name for e in experiments][0] == 'my-experiment'


