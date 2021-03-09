from fabric import Connection,task
import os
import datetime
today = datetime.datetime.now()

CONNECTION_PROPS = {
    "host": "52.237.98.166",
    "user": "ubuntu",
     "connect_kwargs": {
        "key_filename": "/Users/h/thinkvalAzure.pem"
    },
}

# MEDIUM_CONNECTION_PROPS = {
#     "host": "10.0.1.8",
#      "connect_kwargs": {
#         "key_filename": "thinkvalAzure.pem"
#     },
# }

connections = ["ubuntu@10.0.1.19", "ubuntu@10.0.1.22","ubuntu@10.0.1.23", "ubuntu@10.0.1.24"]
# connections = ["ubuntu@10.0.1.19", "ubuntu@10.0.1.22","ubuntu@10.0.1.23", "ubuntu@10.0.1.24", "ubuntu@10.0.1.20", "ubuntu@10.0.1.21"]
# connections = ["ubuntu@10.0.1.19", "ubuntu@10.0.1.22"]
filesToExtract = ["calculateRuntime", "perspectiveRuntime", "queryProcessor"]

def extractLogsFromWorkers(c):
    index = 1
    for conn in connections:
        print(f'{conn} , {today.strftime("%Y-%m-%d")}')
        sftpcon = Connection(host=conn, connect_kwargs={"key_filename":"/Users/h/thinkvalAzure.pem"}, gateway=c)        
        print(f'{sftpcon} is connected? {sftpcon.is_connected}')
        
        for logToExtract in filesToExtract:
            fileToRetrieve = f'/opt/apps/logs/val-services/{logToExtract}.{today.strftime("%Y-%m-%d")}.json'
            # fileToRetrieve = f'/opt/apps/logs/val-services/calculateRuntime.2021-02-08.json'
            # print(f'{fileToRetrieve} , {index}')
            # retrieve file
            try:
                sftpcon.get(fileToRetrieve, f'workers/worker{index}/{logToExtract}.json')
                print(f'Retrived to locate {fileToRetrieve} in worker{index}')
            except:
                print(f'Unable to locate {fileToRetrieve} in worker{index}')
                continue
        
        index += 1

@task
def accessMediumJobVM(c):
    print("access medium job vm")
    extractLogsFromWorkers(c);

@task 
def webserverAccess(ctx):
    print(ctx)
    print(ctx.is_connected)
    accessMediumJobVM(ctx)
    
@task
def deploy(ctx):
    for logToExtract in filesToExtract: 
        open(f'{logToExtract}.{today.strftime("%Y-%m-%d")}.json', "w")
        
    with Connection(**CONNECTION_PROPS) as c:
        print("access webserver vm")
        webserverAccess(c)