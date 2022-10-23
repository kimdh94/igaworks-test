# -*- coding: utf-8 -*-
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import json, io, os
from s3_conn import s3_connection
from datetime import datetime


# csv 파일 일자별 파싱하여 생성 후 일자별 리스트 반환
def csv_parsing(obj, file_name, src_path, chunksize, header, dtype):
    print("--- csv parsing start ---")
    ymd_list =[]

    csv_df = pd.read_csv(io.BytesIO(obj["Body"].read()), chunksize=chunksize, low_memory=False, names=header, dtype=dtype)

    # 일자별로 파싱된 csv 파일 생성
    for i, chunk in enumerate(csv_df):
        print("parsing chunk", i)
        # ymd 컬럼 추가 및 인덱스 설정
        chunk['ymd'] = chunk['server_datetime'].astype(str).str[0:10]
        chunk.set_index('ymd', inplace=True)
        sort_chunk = chunk.sort_index()
        
        # 중복 제거를 위해 set -> list
        chunk_ymd_list = list(set(sort_chunk.index.to_list()))
        # 전체 리스트에 추가
        ymd_list.extend(chunk_ymd_list)
        
        for dt in chunk_ymd_list:
            # 일자별 dataframe 추출 및 저장
            tmp_df = sort_chunk.loc[dt]
            
            # csv 파일명
            tmp_file_name = file_name +'_' + dt

            # 파일이 없을 경우 생성, 있을 경우 이어쓰기
            if not os.path.exists(tmp_file_name):
                tmp_df.to_csv(src_path+tmp_file_name, index=False, mode='w', header=False, quoting=1)
            else:
                tmp_df.to_csv(src_path+tmp_file_name, index=False, mode='a', header=False, quoting=1)

    # 모든 일자 리스트
    ymd_list = list(set(ymd_list))

    print("--- csv parsing end ---")

    return ymd_list


# S3 - csv 파일 parquet 변환
def csv_to_parquet(file_name, csv_file_path, src_path, parquet_file_path, upload_file_path, chunksize, header, dtype, parse_dates, parquet_schema):
    # S3 연결 및 객체 가져오기
    s3 = s3_connection()
    bucket_name = "igaworks-s3-bucket"
    obj = s3.get_object(Bucket=bucket_name, Key=csv_file_path+file_name)

    # csv 파일 일자별 파싱 및 일자별 리스트 가져오기
    ymd_list = csv_parsing(obj, file_name, src_path, chunksize, header, dtype)

    dt_parser = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
    
    # 일자별 csv -> parquet 변환 및 s3 업로드
    for ymd in ymd_list:
        # src/(Attribution 또는 Event)/(Attribution 또는 Event)_yyyymmdd - 일별로 파싱한 csv 파일
        src_file = src_path+file_name+'_'+ymd 

        # dest/(Attribution 또는 Event)/(Attribution 또는 Event)_yyyymmdd - 파싱된 csv 파일로 parquet 변환
        parquet_file = parquet_file_path+file_name+'_'+ymd

        # dest/(Attribution 또는 Event)/yyyymmdd/(Attribution 또는 Event)_yyyymmdd - S3 업로드 path
        upload_file = upload_file_path+ymd+'/'+file_name+'_'+ymd

        csv_stream = pd.read_csv(src_file, chunksize=chunksize, low_memory=False, names=header, dtype=dtype, parse_dates=parse_dates, date_parser=dt_parser)

        # parquet 변환 작업
        for i, chunk in enumerate(csv_stream):
            print("Chunk", i)
            if i == 0:
                parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
                
            table = pa.Table.from_pandas(chunk, schema=parquet_schema)
            parquet_writer.write_table(table)

        parquet_writer.close()

        # s3 업로드
        s3.upload_file(parquet_file, bucket_name, upload_file)


def dir_make(path):
    if not os.path.exists(path):
        os.makedirs(path)


def main():
    try:
        # 인자 받기
        arg = sys.argv[1]

        # Attribution Data
        if arg=='1':
            from attribution_data import schema

            file_path = 'json_file/attribution_data.json'
            parquet_schema = schema()

        # Event Data
        elif arg=='2':
            from event_data import schema

            file_path = 'json_file/event_data.json'
            parquet_schema = schema()

        else:
            raise Exception('잘못된 입력')

        # json 파일 읽기 및 변수 할당
        with open(file_path, encoding='UTF-8') as json_file:
            data = json.load(json_file)

        file_name = data["file_name"]
        csv_file_path = data["csv_file_path"]
        parquet_file_path = data["parquet_file_path"]
        upload_file_path = data["upload_file_path"]
        header = data["header"]
        dtype = data["dtype"]
        parse_dates = data["parse_dates"]
        chunksize = data["chunksize"]

        # 필요한 디렉토리 생성
        src_path = 'src/'+file_name+'/'
        dest_path = 'dest/'+file_name+'/'

        dir_make(src_path)
        dir_make(dest_path)


        # 변환 함수 호출
        csv_to_parquet(file_name, csv_file_path, src_path, parquet_file_path, upload_file_path, chunksize, header, dtype, parse_dates, parquet_schema)

        # 만들어진 parquet 데이터 조회
        # df = pd.read_parquet(parquet_file, engine='pyarrow')
        # print(df) 
    
    # except IndexError :
    #     print('인자를 입력해 주세요.')

    except Exception as e :
        print('예외 발생', e)
        

if __name__ == '__main__':
    main()