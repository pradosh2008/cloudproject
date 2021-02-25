partitionKeys=['year','month']
def con(connection_options={"partitionK": partitionKeys}):
    #connection_options = {"partitionKeys": partitionKeys}
    print(connection_options['partitionK'])

con(connection_options=partitionKeys)