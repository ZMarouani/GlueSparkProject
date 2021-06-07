import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('glue')
    crawler_response = client.start_crawler(Name='s3DataCrawler')
    print(crawler_response)
    #client.start_job_run(JobName='string', Arguments= {})
    
    return {
        'statusCode': 200,
        'body': json.dumps('Crawler and jobs started !')
    }
