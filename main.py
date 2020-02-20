import time, boto3, json
from botocore.exceptions import ClientError

SQS_ENDPOINT_FILENAME = '/etc/queue-endpoint'
DYNAMO_TABLENAME_FILENAME = '/etc/dynamo-tablename'

def main():
    queue = get_queue()
    table = get_table()
    while True:
        process_queue_message(queue, table)
        time.sleep(1)
    
def read_file(filename):
    file = open(filename)
    
    if file.mode != 'r':
        print("ERROR: Could not open file ${filename} for reading.")
        exit(status=2)
    
    return file.read().splitlines()[0]

def get_queue():
    SQS_ENDPOINT = read_file(SQS_ENDPOINT_FILENAME)
    print('INFO: SQS Queue Endpoint: ', SQS_ENDPOINT)
    
    sqs = boto3.resource('sqs')
    queue = sqs.Queue(SQS_ENDPOINT)
    
    return queue
        
def get_table():
    DYNAMO_TABLENAME = read_file(DYNAMO_TABLENAME_FILENAME)
    print('INFO: DynamoDB table name: ', DYNAMO_TABLENAME)
    
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(DYNAMO_TABLENAME)
    
    return table

def process_queue_message(queue, table):
    messages = queue.receive_messages(
        MaxNumberOfMessages=1,
        VisibilityTimeout=10,
        WaitTimeSeconds=15
    )
    
    if len(messages) <= 0:
        print('INFO: No messages in queue')
        return
    
    for message in messages:
        vote = ''
        
        try:
            body = json.loads(message.body)
            print('INFO: Message body: ', body)
            vote = body['Vote']
        except (KeyError, ValueError):
            print('ERROR: Malformed message with ID', message.message_id, ', discarding.')
            message.delete()
            continue
        
        # We could do some slow processing here that wouldn't be suited for lambda
        time.sleep(1)
        
        success = False
        # Record the vote in DynamoDB
        try:
            table.update_item(
                Key={
                    '_id': vote,
                },
                UpdateExpression='ADD votes :val',
                ConditionExpression=boto3.dynamodb.conditions.Attr('_id').eq(vote),
                ExpressionAttributeValues={
                    ':val': 1
                }
            )
            success = True
        except ClientError as e:  
            if e.response['Error']['Code']=='ConditionalCheckFailedException':
                print('WARNING: Vote item does not exist')
            else: 
                raise e
            
        if success:
            print('INFO: Vote recorded successfully')
        
        # Remove message from queue
        message.delete()
    
        
if __name__ == "__main__":
    main()