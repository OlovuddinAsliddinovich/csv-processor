import csv, io, os, time, boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = os.environ['BUCKET_NAME']
    key = os.environ["KEY"]

    obj = s3.get_object(Bucket=bucket, Key=key)
    print(obj)
    body = obj['Body'].read().decode('utf-8').splitlines()
    reader = csv.reader(io.StringIO('\n'.join(body)))

    out_rows = []

    for row in reader:
        if len(row) < 3:
            continue
        r = [c.strip().upper() for c in row]
        out_rows.append(''.join(r))
    out_data = '\n'.join(out_rows).encode('utf-8')
    s3.put_object(Bucket=bucket, Key='output/clean.csv', Body=out_data)
    print("Done")
    return {
        "rows": len(out_rows)
    }