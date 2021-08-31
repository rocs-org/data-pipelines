cd data-pipelines
git checkout main
git pull
make build
make setup
bash wait-for-healthy-container.sh airflow-scheduler 120

