# How to store OHLC time-series data: a SQL vs No-SQL vs Hybrid comparison
Complementary to the post at:

### Dependencies
* Python 3.8.6+
* [Install](https://www.postgresql.org/download/) PostgreSQL 12. Then run `start_and_setup_postgres.sh` which should create a `postgres` database with a `postgres` user and start the service.

  **Note**: version 13 is not supported by TimescaleDB at the time of writing.
* MongoDB ([installation page](https://docs.mongodb.com/manual/installation/)) with a `performance` database.
* TimescaleDB, _just follow the_ [installation page](https://docs.timescale.com/latest/getting-started/installation), and good luck.

### Run
After installing dependencies, e.g. `pip install -r requirements.txt` or `pipenv install`, just:
```
python main.py
```
will take a few minutes.

You can tweak some settings, namely:
```
settings = {
    'num_prefill': 1000,
    'iterations': 100,
    'iterations_looped': 1,
    'records': 1000
}
```
where:
* `num_prefill` is the number of whole series to pre-fill the database in order to measure storage
* `iterations` is the number of operations to repeat an insert, read, delete, append
* `iterations_looped` is the number of times the group of same operations are repeated
* `records` is the number of records to append (other than 1)

### The data
We download from Yahoo the OHLC data for Amazon on first run, then it's cached. The series has about 6000 daily records (or observations). Hence, when we insert a series, we add the whole 6000 records. When we append, we add some records to the bottom of an existing series.

The pre-fill generates random IDs but reuses the same Amazon series.


### Contributing
To add tests, create a runner e.g. `MySqlRunner` taking inspiration from `mongodb_runner.py` or `postgres_runner.py`. The runner should:
* subclass the `BaseRunner`
* have an `__enter__()` method which creates the library/collection/table
* have an `__exit__()` method which removes the library/collection/table
* have a `get_storage_size()` method which measure the space on disk of the library/collection/table
* have the `write_one()`, `read_one()`, `append_one()`, `delete_one()` methods
* have a `list_tickers()` method

Then add the runner under the `main()` method in `main.py`.
