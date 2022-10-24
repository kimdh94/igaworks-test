## 설명
json파일을 참조하여 AWS S3 연결
연동된 S3의 csv파일을 일자별로 파싱 후 생성(src 디렉토리)
생성된 csv파일을 읽어 parquet파일로 생성(dest 디렉토리)
생성된 parquet파일 S3로 업로드


## 실행 방법
- Attribution 데이터 작업시

    python main() 1


- Event 데이터 작업시

    python main() 2

