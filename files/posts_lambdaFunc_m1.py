import json
import boto3 
import uuid 

def createPost(event, table):
    req_body = json.loads(event['body'])
    if req_body is None:
        err = f"body is empty, please supply elemets."
        return err, 1
    error = []
    item = {}
    # checking for title 
    if req_body['title'] != "":
        item.update({'title':req_body['title']})
    else:
        error.append("'title' not provided.")
    # checking for body 
    if req_body['body'] != "":
        item.update({'body':req_body['body']})
    else:
        error.append("'body' not provided.")
    # checking for category
    if req_body['category'] != "":
        item.update({'category':req_body['category']})
    else:
        error.append("'category' not provided.")
    # checking for likes
    if req_body['likes'] != "":
        item.update({'likes':req_body['likes']})
    else:
        error.append("'likes' not provided.")
    # checking for tags
    if req_body['tags'] != "":
        item.update({'tags':req_body['tags']})
    else:
        error.append("'tags' not provided.")    
    if len(error) == 0:
        # write the data into the dynamdoDB with generated id 
        # generate Id 
        myId = str(uuid.uuid4())
        item.update({'id':myId})
        table.put_item(Item=item)
        return item, 0
    else:
        return error, 1

def updatePost(event, table):
    req_body = json.loads(event['body'])
    if req_body is None:
        err = f"body is empty, please supply elemets."
        return err, 1
    error = []
    item = {}
    # checking for id 
    if req_body['id'] != "":
        item.update({'id':req_body['id']})
    else:
        error.append("'id' not provided.")
    # checking for title 
    if req_body['title'] != "":
        item.update({'title':req_body['title']})
    else:
        error.append("'title' not provided.")
    # checking for body 
    if req_body['body'] != "":
        item.update({'body':req_body['body']})
    else:
        error.append("'body' not provided.")
    # checking for category
    if req_body['category'] != "":
        item.update({'category':req_body['category']})
    else:
        error.append("'category' not provided.")
    # checking for likes
    if req_body['likes'] != "":
        item.update({'likes':req_body['likes']})
    else:
        error.append("'likes' not provided.")
    # checking for tags
    if req_body['tags'] != "":
        item.update({'tags':req_body['tags']})
    else:
        error.append("'tags' not provided.")    
    if len(error) == 0:
        # write the data into the dynamdoDB 
        # updating items
        UpdateExpression = 'SET likes = :l, category = :c, tags = :tg, title = :tt, body = :b'
        ExpressionAttributeValues = {
            ':l': item['likes'],
            ':c': item['category'],
            ':tg': item['tags'],
            ':tt': item['title'],
            ':b': item['body']
        }
        update = table.update_item(
            Key={
                'id': item['id']
            },
            ConditionExpression= 'attribute_exists(id)',
            UpdateExpression=UpdateExpression,
            ExpressionAttributeValues=ExpressionAttributeValues
        )
        # if updated successfully then fetch latest data
        if update['ResponseMetadata']['HTTPStatusCode'] == 200:
            res=table.get_item(Key={"id":item['id']}) 
        return res['Item'], 0
    else:
        return error, 1

def deletePost(event, table):
    # default_limit
    if 'queryStringParameters' in event and event['queryStringParameters'] is not None and 'id' in event['queryStringParameters']:
        id = int(event['queryStringParameters']['id'])  
        res=table.delete_item(Key={"id":id})
        statusCode = res['ResponseMetadata']['HTTPStatusCode']
        return statusCode, f"Item Deleted"
    else:
        return 200, f"Error: Posts ID is not provided!!"

def getPost(event, table):
    # default_limit
    limit: int = 4
    if 'queryStringParameters' in event and event['queryStringParameters'] is not None and 'limit' in event['queryStringParameters']:
        limit = int(event['queryStringParameters']['limit'])  
    result = table.scan(Limit=limit)
    return result["Items"]

def lambda_handler(event, context):
    db = boto3.resource('dynamodb',region_name='us-east-1')
    table = db.Table('posts')
    # setting up return value 
    http_res = {}
    http_res['headers'] = {}
    http_res['headers']['Content-Type'] = 'application/json'
    if event['httpMethod'] == "GET":
        res = getPost(event, table)
        # return value 
        http_res['statusCode'] = 200
        http_res['body'] = json.dumps(str(res))
    elif event['httpMethod'] == "POST":
        res, status = createPost(event, table)
        if status == 0:
            # return value 
            http_res['statusCode'] = 200
            http_res['body'] = json.dumps(str(res))
        else:
            # return value 
            http_res['statusCode'] = 200
            http_res['body'] = json.dumps(str(res))
    elif event['httpMethod'] == "PUT":
        res, status = updatePost(event, table)
        if status == 0:
            # return value 
            http_res['statusCode'] = 200
            http_res['body'] = json.dumps(str(res))
        else:
            # return value 
            http_res['statusCode'] = 200
            http_res['body'] = json.dumps(str(res))
    elif event['httpMethod'] == "DELETE":
        status, res = deletePost(event, table)
        # return value 
        http_res['statusCode'] = status
        http_res['body'] = json.dumps(str(res))
    else:
        # return value 
        http_res['statusCode'] = 200
        http_res['body'] = json.dumps("Could not understand the HTTP Method. Please Try again with CREATE, UPDATE, DELETE and GET")
    return http_res
