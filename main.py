import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import json, io
from s3_conn import s3_connection


# S3 - csv 파일 parquet 변환
def csv_to_parquet(csv_file, parquet_file, upload_file, chunksize, header, dtype, parse_dates, parquet_schema):
    # S3 연결 및 객체 가져오기
    s3 = s3_connection()
    bucket_name = "igaworks-s3-bucket"
    obj = s3.get_object(Bucket=bucket_name, Key=csv_file)

    # csv 파일 읽기
    # csv_stream = pd.read_csv(csv_file, chunksize=chunksize, low_memory=False, names=header, dtype=dtype, parse_dates=parse_dates)
    csv_stream = pd.read_csv(io.BytesIO(obj["Body"].read()), chunksize=chunksize, low_memory=False, names=header, dtype=dtype, parse_dates=parse_dates)

    # parquet 변환
    for i, chunk in enumerate(csv_stream):
        print("Chunk", i)
        if i == 0:
            parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
            
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)

    parquet_writer.close()

    # parquet 파일 S3에 업로드
    s3.upload_file(parquet_file, bucket_name, upload_file)


def main():
    try:
        # 인자 받기
        arg = sys.argv[1]

        # Attribution Data
        if arg=='1':
            from attribution_data import schema

            file_path = 'attribution_data.json'
            parquet_schema = schema()

        # Event Data
        elif arg=='2':
            from event_data import schema

            file_path = 'event_data.json'
            parquet_schema = schema()

        else:
            raise Exception('잘못된 입력')

        # json 파일 읽기 및 변수 할당
        with open(file_path, encoding='UTF-8') as json_file:
            data = json.load(json_file)

        csv_file = data["csv_file"]
        parquet_file = data["parquet_file"]
        upload_file = data["upload_file"]
        header = data["header"]
        dtype = data["dtype"]
        parse_dates = data["parse_dates"]
        chunksize = data["chunksize"]

        # 변환 함수 호출
        csv_to_parquet(csv_file, parquet_file, upload_file, chunksize, header, dtype, parse_dates, parquet_schema)

        # 만들어진 parquet 데이터 조회
        # df = pd.read_parquet(parquet_file, engine='pyarrow')
        # print(df) 
    
    # except IndexError :
    #     print('인자를 입력해 주세요.')

    except Exception as e :
        print('예외 발생', e)
        

if __name__ == '__main__':
    main()