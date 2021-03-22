import requests
import boto3
import codecs
import re
import json
import time
import datetime


def lambda_handler(event, context):
        
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')

    bucket = s3_resource.Bucket('s3-bucket-example')

    prefix = bucket.objects.filter(Prefix='cef-logs')

    files = [obj.key for obj in sorted(prefix, key=lambda x: x.last_modified)][-2]

    print(files)

    s3_file = str(files).lstrip('[').rstrip(']').lstrip('\'').rstrip('\'')

    print(s3_file)

    s3_object = s3_resource.Object('s3-bucket-example', s3_file)

    line_stream = codecs.getreader("utf-8")

    lst=[]

    for line in line_stream(s3_object.get()['Body']):
        if re.match("CEF:0", line):
            cef = line
        
            today = datetime.date.today()
            today_string = time.strftime('%Y-%m-%d.%H%M%S')
        
            cefFields = ['version', 'device_vendor', 'device_product', 'device_version', 'signature_id', 'name', 'severity', 'extension']

            def recordToJson(record):
                regex = re.compile(r'''     
                    [\S]+=                 
                    (?:
                    \s*                    
                    (?!\S+=)\S+         
                    )+                      
                    ''', re.VERBOSE)
                extension = dict()
                for pair in regex.findall(record):
                    split = pair.split('=',1)
                    extension[split[0]]=split[1]

                cefData = record.split("| ")[0].split("|")
                cefData.append(extension)
                cefRecord = dict(zip(cefFields, cefData))

                return cefRecord
        
            def linesToJson(lines):
                json = ""
                records = lines.split("\n")

                for line in records:
                    if len(line) > 0:
                        json += str(recordToJson(line)) + '\n'
                return json
        
            data = recordToJson(cef)

            data['service']='imperva'
        
            data['ddsource']='s3-imperva-logs'
        
            data['ddtags']='env:production'
        
            lst.append(data)
    
    today = datetime.date.today()
    today_string = time.strftime('%Y-%m-%d.%H%M%S')

    list = str(lst)[1:-1]

    json = str(list).replace("'", '"')
    
    filepath ='json-logs/imperva-' + today_string + '.json'
    
    s3_client.put_object(Bucket='s3-bucket-example', Key=filepath, Tagging='Use=Imperva-Logs&env=production', Body=(json))

    for l in lst:
     
         ddog = str(l).replace("'", '"')

         headers = {'DD-API-KEY': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXX', 'Content-Type': 'application/json'}

         r = requests.post("https://http-intake.logs.datadoghq.com/v1/input", data=ddog, headers=headers)

         print(r)
