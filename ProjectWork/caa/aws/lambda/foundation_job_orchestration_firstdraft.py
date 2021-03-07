import json
from urllib.parse import unquote_plus
import boto3
import uuid


print("STARTING NEW INVOCATION!!!")
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
glue = boto3.client('glue')

def lambda_handler(event, context):
    # Setting variables to equal values from the event object passed in.
    source_bucket_name = event['Records'][0]['s3']['bucket']['name']
    region = event['Records'][0]['awsRegion']
    object = event['Records'][0]['s3']['object']['key']
    user = event['Records'][0]['userIdentity']['principalId']
    
    #key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    table_nam = object.split('/')[-2]
    print("table name " + table_nam)
    
    
    # download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
    # upload_path = '/tmp/resized-{}'.format(tmpkey)
    src = s3_resource.Bucket(source_bucket_name)
    #file_Prefix = 'plus/pk/'
    gluejobname = "gj_fdn_plus_"+table_nam+"_dev"
    #gluejobname="demo1"
    print("glue job name : " + gluejobname)    

    # for obj in src.objects.filter(Prefix=file_Prefix):
    #     if(obj.size != 0):
    #         file_name = obj.key.split('/')[-1]
    #         print('source table name : ', file_name)
    
    
    
    
    print("Bucket: " + source_bucket_name)
    print("Region: " + region)
    print("User is " + user)
    print("Object is " + object)
    #print("UnquoteObject is " + key)
    # print(uuid.uuid4())
    # print("download_path  " + download_path)
    # print("upload_path  " + upload_path)
    
    #status = glue.get_job_run(JobName=gluejobname)
    #print("Job Status : ", status['JobRun']['JobRunState'])
    

    currentjobrun = glue.start_job_run(JobName=gluejobname)
    print ("GLUE_JOB run ID: " + currentjobrun['JobRunId'])
    
    status = glue.get_job_run(JobName=gluejobname, RunId=currentjobrun['JobRunId'])
    print("Job Status : ", status['JobRun']['JobRunState'])
    
    return(object)
	
	
	
	
#some rough work

    while True:
        status = glue.get_job_run(JobName=gluejobname, RunId=currentjobrun['JobRunId'])
        state=status['JobRun']['JobRunState']
        if state == 'SUCCEEDED':
            print("Glue job name:  {} job id: {} is {}", status['JobRun']['JobName'], status['JobRun']['Id'],status['JobRun']['JobRunState'])
            return
        
    #print("Job Status : ", status['JobRun']['JobRunState'])
    
    #return(object)