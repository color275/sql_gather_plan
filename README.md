# Architecture
![Architecture](img/arch.png)

# Service 구성

### Performance Insgiht
...

### Lambda
- sql_gather.py
    - Performance Insight API 를 통해 SQL를 S3로 수집
- sql_plan.py
    - S3 에 수집된 SQL 을 각 버전의(v5.7, v8.0) DB에서 Plan 수행 후 결과를 S3로 수집
...

### DynamoDB
Lamdba에서 중복 SQL이 수집되지 않도록 Tokenized ID 를 저장
![Alt text](img/dynamodb.png)


### S3
- sql_fulltext/
    - SQL 수집
- sql_plan/
    - Plan 결과 수집

![Alt text](img/s3.png)

### Athena
SQL를 통해 데이터 조회
![Alt text](img/athena.png)

### QuickSight
- 전체 SQL 리스트, Plan Diff 확인
- 각 버전에서의 Plan 결과 및 차이 확인
- SQL FullText 확인

![Alt text](img/quicksight.png)
