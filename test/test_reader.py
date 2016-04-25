from parquet import ParquetReader


def test_int_bool_dataset():
    reader = ParquetReader('test-data/int_bool.parquet')
    dataframe = reader.read()
    assert dataframe['int'].dtype == 'int64'
    assert dataframe['bool'].dtype == 'bool'
    assert dataframe['int'].tolist() == [1, 2, 3, 4, 5, 6]
    assert dataframe['bool'].tolist() == [
        False, True, False, False, True, True]
