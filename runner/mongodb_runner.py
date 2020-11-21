from runner.base_runner import BaseRunner


class MongodbRunner(BaseRunner):
    def __enter__(self):
        super().__enter__()
        self.collection = self.db.create_collection('eod')
        self.collection.create_index('ticker')
        self.collection.insert_one({'ticker': None})
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.collection.drop()

    def get_storage_size(self):
        return self.db.command('collstats', 'eod', scale=1024*1024)['totalSize']

    def to_data(self, df):
        return df.to_dict(orient='list')

    def write_one(self, df):
        data = self.to_data(df)
        data['ticker'] = self.get_random_int()
        self.collection.insert_one(data)

    def list_tickers(self):
        return list(filter(None, self.collection.distinct('ticker')))

    def read_one(self, ticker):
        return self.collection.find_one({'ticker': ticker})

    def delete_one(self, ticker):
        self.collection.remove({'ticker': ticker})

    def append_one(self, ticker, df):
        data = self.to_data(df)
        self.collection.update(
            {'ticker': ticker},
            {'$push': {
                field: {'$each': values}
                for field, values in data.items()
            }}
        )


class ArcticRunner(BaseRunner):
    def __enter__(self):
        super().__enter__()
        self.db.initialize_library('eod')
        self.collection = self.db['eod']
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.delete_library('eod')

    def get_storage_size(self):
        return self.collection.stats()['totals']['size']/(1024*1024)

    def write_one(self, df):
        ticker = str(self.get_random_int())
        self.collection.write(ticker, df)

    def list_tickers(self):
        return self.collection.list_symbols()

    def read_one(self, ticker):
        return self.collection.read(ticker).data

    def delete_one(self, ticker):
        self.collection.delete(ticker)

    def append_one(self, ticker, df):
        self.collection.append(ticker, df, upsert=False)
