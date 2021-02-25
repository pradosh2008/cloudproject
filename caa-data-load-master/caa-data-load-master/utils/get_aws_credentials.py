from boto3 import Session

session = Session(region_name='us-east-1', profile_name='automationuser')  #
credentials = session.get_credentials()
# Credentials are refreshable, so accessing your access key / secret key
# separately can lead to a race condition. Use this to get an actual matched
# set.
current_credentials = credentials.get_frozen_credentials()

# I would not recommend actually printing these. Generally unsafe.
print(current_credentials.access_key)
print(current_credentials.secret_key)
print(current_credentials.token)
