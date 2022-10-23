import pyarrow as pa

def schema():
    parquet_schema = pa.schema([
        ('identity_adid', pa.string()),
        ('os', pa.string()),
        ('model', pa.string()),
        ('country', pa.string()),
        ('event_name', pa.string()),
        ('log_id', pa.string()),
        ('server_datetime', pa.timestamp('ms', tz='Asia/Seoul')),
        ('quantity', pa.int64()),
        ('price', pa.decimal128(10,1)),
    ])

    return parquet_schema


if __name__ == '__main__':
    print(__name__)