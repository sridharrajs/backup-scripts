import boto3
import logging
import os
import humanize

from botocore.client import Config
from botocore.exceptions import ClientError

# Fill your s3 details
AWS_S3 = {
    'ACCESS_KEY_ID': 'YOUR_ACCESS_KEY_ID',
    'SECRET_ACCESS_KEY': 'YOUR_SECRET_ACCESS_KEY',
    'SIGNATURE_VERSION': 's3v4',
    'BUCKET_NAME': 'YOUR_BUCKET_NAME',
    'REGION': 'REGION_NEAR_TO_YOU'
}

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_S3['ACCESS_KEY_ID'],
    aws_secret_access_key=AWS_S3['SECRET_ACCESS_KEY'],
    config=Config(signature_version=AWS_S3['SIGNATURE_VERSION'], region_name=AWS_S3['REGION']),
)


def upload_fileto_s3(file_path, s3_folder_name):
    bucket_name = AWS_S3['BUCKET_NAME']
    try:
        _, file_name = os.path.split(file_path)
        key = s3_folder_name + '/' + file_name
        s3_client.upload_file(file_path, bucket_name, key)
        return True
    except ClientError as e:
        logging.error(e)
        return False


def upload_buffer_to_s3(buffer_content, key, extra_args={}):
    print(extra_args)
    bucket_name = AWS_S3['BUCKET']

    try:
        s3_client.put_object(Body=buffer_content, Bucket=bucket_name, Key=key, ACL="public-read",
                             **{"ContentType": "image/jpeg"})
        return True
    except ClientError as e:
        logging.error(e)
        return False


def get_presigned_s3_url(key, custom_filename):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_S3['ACCESS_KEY_ID'],
            aws_secret_access_key=AWS_S3['SECRET_ACCESS_KEY'],
            config=Config(signature_version=AWS_S3['SIGNATURE_VERSION'], region_name=AWS_S3['REGION'])
        )
        bucket_name = AWS_S3['BUCKET']
        return s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket_name,
                'Key': key,
                'ResponseContentDisposition': "attachment; filename = " + custom_filename
            },
            ExpiresIn=10000
        )
    except ClientError as e:
        logging.error(e)
        return None


def get_s3_bucket_location():
    bucket_name = AWS_S3['BUCKET']
    return s3_client.get_bucket_location(Bucket=bucket_name)['LocationConstraint']


def delete_object(path):
    return s3_client.delete_object(Bucket=AWS_S3['BUCKET'], Key=path)


if __name__ == '__main__':
    # Absolute path to the folder which should be uploaded
    folder_path = '/usr/home/sridharrajs/somefolder'
    # folder name under which the files should be uploaded to
    s3_folder_name = 'somefolder'

    file_entries = []
    print(f'Scanning {folder_path} for files...')
    estimated_size = 0
    with os.scandir(folder_path) as itr:
        for entry in itr:
            file_name = entry.name
            if not file_name.startswith('.') and os.path.isfile(entry):
                file_entries.append(entry)
                estimated_size = estimated_size + os.path.getsize(entry.path)

    print(f'Total files available for upload {len(file_entries)}, estimated size {humanize.naturalsize(estimated_size)}')
    print(f'|File Name \t| Size \t\t| Status|')
    print(f'|----------\t|------\t\t|-------|')
    for entry in file_entries:
        file_absolute_path = entry.path
        file_size = humanize.naturalsize(os.path.getsize(file_absolute_path))
        print(f'|{entry.name}\t| {file_size} \t| Uploaded|')
        upload_fileto_s3(file_absolute_path, s3_folder_name)

    print('Done')

