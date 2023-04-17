#!/usr/bin/python3

import json
import boto3
import uuid

def main():
    # create resource client session
    db = boto3.resource('dynamodb',region_name='us-east-1')
    table = db.Table('posts')

    # read the json data and upload the file
    with open('data.json') as json_file:
            data = json.load(json_file)

    # Add id in the json data
    for item in data:
        myId = str(uuid.uuid4())
        item["id"]=myId

    # Insert all the items/row
    for i in data:
        table.put_item(Item=i)

    # Fetch/scan all the items
    res=table.scan()

    # loop through items
    for i in res["Items"]:
        print(i)

if __name__ == "__main__":
    main()
