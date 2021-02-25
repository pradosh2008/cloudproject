


glue_client = boto3.client(
    'glue',
    region_name = 'us-east-1'
)


crawler_lists = glue_client.list_crawlers(MaxResults=1000)
available_crawlers = crawler_lists["CrawlerNames"]
crawler_name=target_tbl+'_crawler'

if crawler_name not in available_crawlers :
#Attempt to create and start a glue crawler on PSV table or update and start it if it already exists.
    glue_client.create_crawler(
        Name = crawler_name,
        Role = 'CAA-Service-Glue',
        DatabaseName = 'work',
        Targets =
        {
            'S3Targets':
            [
                {
                    'Path':'s3://'+target_bucket_name+target_prefix+"/"+target_tbl
                }
            ]
        }
    )
    glue_client.start_crawler(
        Name = crawler_name
    )
else:

    glue_client.start_crawler(
        Name = crawler_name
    )