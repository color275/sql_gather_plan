import boto3
import time
from datetime import datetime
# from base64 import encode
import pytz 
from pprint import pprint
import sys
import json


region = 'ap-northeast-2'

pi_client = boto3.client('pi', region)
s3 = boto3.client('s3', region_name=region)
rds_client = boto3.client('rds', region)
# cw_client = boto3.client('cloudwatch', region)
dynamodb = boto3.resource('dynamodb', region_name=region)

korea_tz = pytz.timezone('Asia/Seoul')

api_call_count = []
api_call_count.append(0)



def get_sql_detail(db_identifier, groupidentifier) :
    response = pi_client.get_dimension_key_details(
        ServiceType='RDS',
        Identifier=db_identifier,
        Group='db.sql',
        GroupIdentifier=groupidentifier,
        RequestedDimensions=[
            'db.sql.statement'
        ]
    )
    
    for metric in response['Dimensions'] :
        if metric.get('Value') :
            sql = metric['Value']
            break    

    print("# call get_dimension_key_details")
    api_call_count[0] += 1
    return sql
    # return sql.replace('\n', ' ').replace('\r', '')   

def get_sql(db_identifier, start_time, end_time, gather_period) :
    
    all_metrics = []
    next_token = None

    while True :
        if next_token :    
            response = pi_client.get_resource_metrics(
                ServiceType='RDS',
                Identifier=db_identifier,
                MetricQueries=[
                    {
                    "Metric": "db.load.avg",
                    "GroupBy": {
                        "Group": "db.sql"
                    }
                }
                ],
                StartTime=start_time,
                EndTime=end_time,
                PeriodInSeconds=gather_period,
                NextToken=next_token
            )

            api_call_count[0] += 1
        else :
            response = pi_client.get_resource_metrics(
                ServiceType='RDS',
                Identifier=db_identifier,
                MetricQueries=[
                    {
                    "Metric": "db.load.avg",
                    "GroupBy": {
                        "Group": "db.sql"
                    }
                }
                ],
                StartTime=start_time,
                EndTime=end_time,
                PeriodInSeconds=gather_period
            )            
            api_call_count[0] += 1

        all_metrics.append(response)
        next_token = response.get('NextToken', None)

        if not next_token :
            break

    return all_metrics

def find_first_sql_command(text):
    positions = {}
    
    for command in ["SELECT", "INSERT", "UPDATE", "DELETE"]:
        pos = text.upper().find(command)
        if pos != -1:
            positions[command] = pos
    
    if positions:
        return sorted(positions, key=positions.get)[0]
    else:
        return "OTHER"

def main():

    ########################################
    ## Variable
    ########################################
    db_identifier_list = [
                            'db-IUJELG26COMQKPV7RDTERN3WR4',
                            'db-ZIBJAVYAOHMU2UHYNWTAVXHWNY'
                        ]
    start_time = time.time() - 70 # 60초 전
    end_time = time.time()
    gather_period = 1
    db_identifier_dict = {}

    dynamodb_table = dynamodb.Table('SqlTokenizedTable') 
    ########################################

    try:
        for db_identifier in db_identifier_list:
            response = rds_client.describe_db_instances()
            
            found = False
            for db_instance in response['DBInstances']:
                if db_instance['DbiResourceId'] == db_identifier:
                    found = True
                    db_identifier_dict[db_identifier] = [db_instance['DBClusterIdentifier'], db_instance['DBInstanceIdentifier']]
                    print(f"DB INFO: {db_identifier_dict[db_identifier]}")
                    
                    if not db_instance.get('PerformanceInsightsEnabled', False):
                        print(f"Performance Insights is disabled for instance: {db_instance['DBInstanceIdentifier']}")
                        sys.exit(1)  # Performance Insights가 비활성화된 경우 프로그램 종료
                    break
                    
            if not found:
                print(f"No matching RDS instance found for the provided resource ID: {db_identifier}")
                sys.exit(1)
    except Exception as e:
        print(f"Error fetching RDS instances: {e}")
        sys.exit(1)


    for db_identifier, db_info in db_identifier_dict.items() :
        sql_list = []
        full_text_list = []

        
        all_metrics = get_sql(db_identifier, start_time, end_time, gather_period)

        for response in all_metrics :

            for metric_response in response['MetricList'] :

                metric_dict = metric_response['Key']
                if metric_dict.get('Dimensions') :
                    sql_info = metric_dict['Dimensions']
                    db_sql_id = sql_info['db.sql.id']
                    db_sql_statement = sql_info['db.sql.statement']
                    db_sql_tokenized_id = sql_info['db.sql.tokenized_id']        
                    
                    v = 0
                    before_v = 0
                    timestamp = ""
                    if metric_response['DataPoints'] :
                        datapoints = metric_response['DataPoints']
                        for datapoint in datapoints :               
                            
                            if datapoint.get('Value') :
                                before_v = datapoint['Value']

                                if v is None or v < before_v :
                                    v = before_v
                                    timestamp = datapoint['Timestamp']
                            
                    
            #         print(f"""
            # # db_sql_id : {db_sql_id}
            # # db_sql_tokenized_id : {db_sql_tokenized_id}
            # # timestamp : {timestamp}
            # # cpu_load : {v}
            # # db_sql_statement : {db_sql_statement}
            #         """)

                    
                    try :
                        # dynamodb 에 저장. db_sql_tokenized_id가 존재하지 않을 경우에만 추가
                        current_time = datetime.now().astimezone(korea_tz).strftime("%Y-%m-%d %H:%M:%S")
                        dynamodb_table.put_item(
                            Item={
                                'db_sql_tokenized_id': db_sql_tokenized_id,
                                'db_identifier': db_identifier,
                                'db_cluster_name': db_info[0],
                                'db_instance_name': db_info[1],
                                'last_update_time': current_time
                            },
                            ConditionExpression='attribute_not_exists(db_sql_tokenized_id)'                      
                        )

                        sql_fulltext = get_sql_detail(db_identifier, db_sql_id)
                        
                        sql_type = find_first_sql_command(sql_fulltext)

                        data = {
                            "db_sql_tokenized_id": db_sql_tokenized_id,
                            "db_sql_id": db_sql_id,
                            "db_identifier": db_identifier,
                            "db_cluster_name": db_info[0],
                            "db_instance_name": db_info[1],
                            "sql_type": sql_type,
                            "sql_fulltext": sql_fulltext,
                            "last_update_time": timestamp.astimezone(korea_tz).strftime("%Y-%m-%d %H:%M:%S"),
                            "cpu_load": v
                        }       
                        

                        # JSON 문자열로 데이터 변환
                        json_data = json.dumps(data)

                        # 날짜 및 시간 포맷 설정
                        current_time = datetime.now().astimezone(korea_tz)
                        year_month_day = current_time.strftime("year=%Y/month=%m/day=%d")
                        # S3 버킷 경로 설정
                        s3_path = f'sql_fulltext/{year_month_day}'
                        file_name = f'{db_sql_tokenized_id}.json'
                        # S3에 파일 업로드
                        s3.put_object(Bucket='chiholee-sql', Key=f"{s3_path}/{file_name}", Body=json_data)

                    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                        # db_sql_tokenized_id가 이미 존재하는 경우 예외 처리
                        # print(f"Skipped existing db_sql_tokenized_id: {db_sql_tokenized_id}")
                        pass

    print("# api_call_count : ", api_call_count[0])
            

if __name__ == "__main__":
	main()