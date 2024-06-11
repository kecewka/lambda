import boto3
import csv
from io import StringIO
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Environment variables
TABLE_NAME = 'trainer_summary'  
BUCKET_NAME = 'askaraytkazin-generic-name-bucket' 

def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    
    # Scan the table to get all items
    response = table.scan()
    items = response['Items']
    
    # Get current month and year
    now = datetime.now()
    current_year = str(now.year)
    current_month = str(now.month)
    
    # Prepare the CSV data
    csv_data = StringIO()
    csv_writer = csv.writer(csv_data)
    
    # Write CSV header
    csv_writer.writerow(['First Name', 'Last Name', 'Training Duration Summary'])
    
    for item in items:
        first_name = item['firstName']
        last_name = item['lastName']
        status = item['status']
        
        # Calculate current month training duration summary
        summary = item.get('summary', {})
        print("summaru:", summary)
        year_summary = summary.get(current_year, {})
        print("year_summary", year_summary)
        training_duration = year_summary.get(current_month, {})
        print("training_duration", training_duration)

        
        # Include in the report if active or (inactive and training duration > 0)
        if status == 'true' or (status == 'false' and int(training_duration) > 0):
            csv_writer.writerow([first_name, last_name, training_duration])
    
    # Generate the file name
    file_name = f"Trainers_Trainings_summary_{now.year}_{now.month:02d}.csv"
    
    # Upload the CSV to S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=csv_data.getvalue(),
        ContentType='text/csv'
    )
    
    return {
        'statusCode': 200,
        'body': f"Report uploaded successfully as {file_name}"
    }
