import boto3
import json


def s3_connection():
    try:
        with open('aws_key.json', encoding='UTF-8') as json_file:
            data = json.load(json_file)

        accesskey = data["accesskey"]
        secret_accesskey = data["secret_accesskey"]
        bucket_name = "igaworks-s3-bucket"

        s3 = boto3.client(
            service_name = "s3",
            region_name="ap-northeast-2",
            aws_access_key_id=accesskey,
            aws_secret_access_key=secret_accesskey
        )
        
    except Exception as e:
        print(e)
    
    else:
        print("s3 buket conneted")
        return s3


if __name__ == '__main__':
    print(__name__)

