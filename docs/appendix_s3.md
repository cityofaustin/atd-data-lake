# Appendix: Acessing S3

*[(Back to Docs Catalog)](index.md)*

This document describes the setup and access of the S3 buckets used to store files for the Data Lake.

## Access S3
First, you'll need to set up your credentials in your account on the system you're wanting to access from. Log in, and run:

```bash
aws configure
```

In the AWS Console, get the key information at "My Security Credentials" under your username drop-down. Use the "Create access key" function. I left the last two options blank.

Here is an example of writing a file to S3 (grabbed from https://stackoverflow.com/questions/40336918/how-to-write-a-file-or-data-to-an-s3-object-using-boto3):

```python
import boto3

some_binary_data = b'Here we have some data'
s3 = boto3.resource('s3')
object = s3.Object('atd-data-lake-raw', '2018/01/test_file1.txt')
object.put(Body=some_binary_data)
```

You can also do:

```python
s3.Object('atd-data-lake-raw', '2018/01/test_file2.txt').put(Body=open('/tmp/hello.txt', 'rb'))
```

Or you can do:

```python
s3.Bucket('atd-data-lake-raw').upload_file('/tmp/hello.txt', '2018/01/test_file3.txt')
```

The documentation at https://dluo.me/s3databoto3 has further examples on writing and reading, while using pandas. Also it has information on how to iterate over all objects under specific directories, which can be very handy for seeing what all is already present in S3.

## Reading from S3
Using the example of "2018/07/05/bt/Austin_bt_07-05-2018.txt" in the "atd-data-lake-raw" bucket, reading that into pandas:

```python
import boto3
import pandas as pd

client = boto3.client('s3')
obj = client.get_object(Bucket='atd-data-lake-raw', Key='2018/07/05/bt/Austin_bt_07-05-2018.txt')
btData = pd.read_csv(obj['Body'], header=None, names=['deviceTime', 'ipAddr', 'fieldTime', 'readerID', 'deviceAddr'])
```

As a sanity check, `btData["readerID"][2]` should be `lamar_parmer`.
