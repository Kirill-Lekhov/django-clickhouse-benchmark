# django-clickhouse-benchmark
A project to test the performance of Clickhouse and Django wrappers

# Setup
1. Run `poetry install`;
2. Create clickhouse db `benchmark` or change connection settings in `settings.py`;
3. Run `poetry run ./manage.py migrate`;
4. Run `poetry run ./manage.py migrate --database='clickhouse'`;
5. Run `poetry run ./manage.py create_fakes` to create fake data;
6. Run `poetry run ./manage.py benchmark` to run benchmark.

# ⚠️ Warning ⚠️
* Some queries may cause lags on weak systems.
