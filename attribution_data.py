import pyarrow as pa

def schema():
    parquet_schema = pa.schema([
        ('partner', pa.string()),
        ('campaign', pa.string()),
        ('server_datetime', pa.timestamp('ms', tz='Asia/Seoul')),
        ('tracker_id', pa.string()),
        ('log_id', pa.string()),
        ('attribution_type', pa.int64()),
        ('identity_adid', pa.string()),
    ])

    return parquet_schema


if __name__ == '__main__':
    print(__name__)