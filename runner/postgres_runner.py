import json

from psycopg2.extras import execute_values

from runner.base_runner import BaseRunner


class BasePostgresRunner(BaseRunner):
    cursor = None

    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.execute('DROP TABLE eod;')
        self.db.commit()
        if self.cursor is not None:
            self.cursor.close()

    def get_storage_size(self):
        self.cursor.execute("SELECT pg_database_size('postgres')")
        return self.cursor.fetchone()[0]/(1024*1024)

    def list_tickers(self):
        self.cursor.execute('SELECT DISTINCT ticker FROM eod;')
        return [record[0] for record in self.cursor.fetchall()]

    def read_one(self, ticker):
        self.cursor.execute(f'SELECT * FROM eod WHERE ticker = {ticker};')
        return self.cursor.fetchone()

    def delete_one(self, ticker):
        self.cursor.execute(f'DELETE FROM eod WHERE ticker = {ticker};')


class PostgresRunner(BasePostgresRunner):
    def __enter__(self):
        super().__enter__()
        self.cursor = self.db.cursor()
        self.cursor.execute('''
            CREATE TABLE eod(
                id SERIAL PRIMARY KEY,
                ticker INTEGER NOT NULL,
                date DATE NOT NULL,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                open DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                adj_close DOUBLE PRECISION
            );''')
        self.cursor.execute('CREATE INDEX idx_eod_ticker ON eod (ticker);')
        self.db.commit()
        return self

    def to_data(self, df):
        return df.to_records(index=False).tolist()

    def write_records(self, ticker, df):
        data = self.to_data(df)
        sql_statement = '''
            INSERT INTO eod (ticker,date,high,low,open,close,volume,adj_close) VALUES %s
        '''
        template = f'({ticker},  %s, %s, %s, %s, %s, %s, %s)'
        execute_values(self.cursor, sql_statement, data, template=template)

        self.db.commit()

    def write_one(self, df):
        ticker = self.get_random_int()
        self.write_records(ticker, df)

    def append_one(self, ticker, df):
        self.write_records(ticker, df)


class PostgresJsonRunner(BasePostgresRunner):
    def __enter__(self):
        super().__enter__()
        self.cursor = self.db.cursor()
        self.cursor.execute('''
            CREATE TABLE eod(
                id SERIAL PRIMARY KEY,
                ticker INTEGER NOT NULL,
                data JSON
            );''')
        self.cursor.execute('CREATE INDEX idx_eod_ticker ON eod (ticker);')
        self.db.commit()
        return self

    def to_data(self, df, orient):
        if orient == 'list':
            return json.dumps(df.to_dict(orient=orient))
        else:
            return df.to_json(orient=orient, index=False)

    def write_one(self, df, orient='split'):
        data = [(self.get_random_int(), self.to_data(df, orient))]

        sql_statement = 'INSERT INTO eod (ticker, data) VALUES %s'
        self.cursor.execute(sql_statement, data)

        self.db.commit()

    def append_one(self, ticker, df):
        new_data = df.to_json(orient='values')
        sql_statement = f"""
            UPDATE eod
            SET data = (data::jsonb || '{new_data}'::jsonb)::json
            WHERE ticker={ticker};
        """

        self.cursor.execute(sql_statement)

        self.db.commit()


class PostgresJsonbRunner(BasePostgresRunner):
    def __enter__(self):
        super().__enter__()
        self.cursor = self.db.cursor()
        self.cursor.execute('''
            CREATE TABLE eod(
                id SERIAL PRIMARY KEY,
                ticker INTEGER NOT NULL,
                data JSONB
            );''')
        self.cursor.execute('CREATE INDEX idx_eod_ticker ON eod (ticker);')
        self.db.commit()
        return self

    def to_data(self, df, orient):
        if orient == 'list':
            return json.dumps(df.to_dict(orient=orient))
        else:
            return df.to_json(orient=orient, index=False)

    def write_one(self, df, orient='split'):
        data = [(self.get_random_int(), self.to_data(df, orient))]

        sql_statement = 'INSERT INTO eod (ticker, data) VALUES %s'
        self.cursor.execute(sql_statement, data)

        self.db.commit()

    def append_one(self, ticker, df, orient='split'):
        '''
        Example query:

        UPDATE eod
        SET data = jsonb_insert(
            data,
            '{data, -1}',
            '[["2020-10-20", 3399.6, 3160.0, 3363.2, 3272.7, 6446500.0, 3272.70],
              ["2020-10-21", 3399.6, 3160.0, 3363.2, 3272.7, 6446500.0, 3272.70]]'::jsonb,
            true)
        WHERE ticker=8942;
        '''
        new_data = df.to_json(orient='values')
        sql_statement = f"""
            UPDATE eod
            SET data = jsonb_insert(
                data,
                '{{data, -1}}',
                '{new_data}'::jsonb,
                true
            )
            WHERE ticker={ticker};
        """

        self.cursor.execute(sql_statement)

        self.db.commit()


class TimescaledbRunner(PostgresRunner):

    def __enter__(self):
        self.cursor = self.db.cursor()
        self.cursor.execute('''
            CREATE TABLE eod(
                ticker INTEGER NOT NULL,
                date DATE NOT NULL,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                open DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                adj_close DOUBLE PRECISION
            );''')
        self.cursor.execute("""
            SELECT create_hypertable('eod', 'date');
            SELECT set_chunk_time_interval('eod', INTERVAL '10y');
            CREATE INDEX idx_eod_ticker ON eod (ticker, date ASC);
            """)
        self.db.commit()
        return self
