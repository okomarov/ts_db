import os
import random
import timeit

import pandas as pd
import pandas_datareader as pdr

import clients
from runner.mongodb_runner import ArcticRunner
from runner.mongodb_runner import MongodbRunner
from runner.postgres_runner import PostgresJsonRunner
from runner.postgres_runner import PostgresJsonbRunner
from runner.postgres_runner import PostgresRunner
from runner.postgres_runner import TimescaledbRunner

settings = {
    'num_prefill': 1000,
    'iterations': 100,
    'iterations_looped': 1,
    'records': 1000
}


def read_or_download(ticker):
    file = os.path.join('data', f'{ticker}.csv')
    if os.path.exists(file) and os.path.getsize(file) > 0:
        data = pd.read_csv(file)
    else:
        data = pdr.get_data_yahoo(ticker.upper(), start='1900-01-01')
        data.to_csv(file)
    return data


def prepare_data():
    tickers = ['AMZN']
    data = {ticker: read_or_download(ticker).round(4) for ticker in tickers}
    return data


def read_from_ticker_list(runner, tickers):
    for ticker in tickers:
        _ = runner.read_one(ticker)


def delete_from_ticker_list(runner, tickers):
    for ticker in tickers:
        _ = runner.delete_one(ticker)


def append_to_ticker_list(runner, tickers, df):
    for ticker in tickers:
        _ = runner.append_one(ticker, df)


def test(runner, data):
    with runner as r:
        series = data['AMZN']
        name = str(runner)
        print(f'{name}\n{"="*len(name)}')

        before = r.get_storage_size()
        r.prefill(series, settings['num_prefill'])
        tickers = random.sample(r.list_tickers(), k=settings['iterations'])
        print(f'Storage (MB): {r.get_storage_size() - before:.1f}')
        res = timeit.timeit(lambda: r.write_one(series), number=settings['iterations'])
        print(f'Total write time (s): {res:.2f}')

        res = timeit.timeit(
            lambda: read_from_ticker_list(r, tickers),
            number=settings['iterations_looped'])
        print(f'Total read time (s): {res:.2f}')

        res = timeit.timeit(
            lambda: append_to_ticker_list(r, tickers, series[-1:]),
            number=settings['iterations_looped'])
        print(f'Total append 1-record time (s): {res:.2f}')

        res = timeit.timeit(
            lambda: append_to_ticker_list(r, tickers, series[-settings['records']:]),
            number=settings['iterations_looped'])
        print(f'Total append 1000-record time (s): {res:.2f}')

        res = timeit.timeit(
            lambda: delete_from_ticker_list(r, tickers),
            number=settings['iterations_looped'])
        print(f'Total delete time (s): {res:.2f}')

        print()


def main():
    data = prepare_data()

    psql_runner = PostgresRunner(clients.get_postgres_db())
    psql_json_runner = PostgresJsonRunner(clients.get_postgres_db())
    psql_jsonb_runner = PostgresJsonbRunner(clients.get_postgres_db())
    timescaledb_runner = TimescaledbRunner(clients.get_postgres_db())
    mongodb_runner = MongodbRunner(clients.get_mongo_db())
    arctic_runner = ArcticRunner(clients.get_mongo_arctic())

    test(psql_runner, data)
    test(psql_json_runner, data)
    test(psql_jsonb_runner, data)
    test(timescaledb_runner, data)
    test(mongodb_runner, data)
    test(arctic_runner, data)


if __name__ == '__main__':
    main()
