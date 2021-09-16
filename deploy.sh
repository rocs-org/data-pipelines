cd data-pipelines
git checkout main
git pull
git secret reveal
mv .env.prod .env
make build
make setup
bash wait-for-healthy-container.sh airflow-scheduler 120

