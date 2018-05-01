import boto3
import config
import io

# http://anthonyfox.io/2017/07/how-i-used-python-and-boto3-to-modify-csvs-in-aws-s3/

S3_KEY = config.Config().S3_KEY
S3_SECRET = config.Config().S3_SECRET
S3_BUCKET = config.Config().S3_BUCKET


class S3ObjectInterator(io.RawIOBase):
    def __init__(self, s3_key, s3_secret, bucket, key):
        """Initialize with S3 bucket and key names"""
        self.s3c = boto3.client('s3', aws_access_key_id = s3_key, aws_secret_access_key = s3_secret)
        self.obj_stream = self.s3c.get_object(Bucket=bucket, Key=key)['Body']

    def read(self, n=-1):
        """Read from the stream"""
        return self.obj_stream.read() if n == -1 else self.obj_stream.read(n)



obj_stream = S3ObjectInterator(S3_KEY, S3_SECRET, S3_BUCKET, "UnderlyingOptionsTradesCalcs_2018-02.csv")

k=0
for line in obj_stream:
    print line
    k+=1
    if k==10:
        break



# obj = s3.get_object(Bucket= "taohonginsight18b", Key= "UnderlyingOptionsTradesCalcs_2018-02.csv") 

# key = boto.connect_s3(S3_KEY, S3_SECRET).get_bucket("taohonginsight18b").get_key("UnderlyingOptionsTradesCalcs_2018-02.csv")
# with smart_open.smart_open(key) as fin:
#     for line in fin:
#         print line

# print obj['Body'].read(5000)


# initial_df = pd.read_csv(io.BytesIO(obj['Body'].read()))

# # print initial_df.shape 

# for line in smart_open.smart_open('s3://taohonginsight18b/UnderlyingOptionsTradesCalcs_2018-02.csv'):
#     print line



# with smart_open.smart_open(obj['Body']) as fin:
#     for line in fin[:10]:
#         print line

    # byte = unfinished_line + byte
    # #split on whatever, or use a regex with re.split()
    # lines = byte.split('\n')
    # unfinished_line = lines.pop()
    # for line in lines:
    #     yield line



# >>> import boto
# >>> from boto.s3.key import Key
# >>> conn = boto.connect_s3('ap-southeast-2')
# >>> bucket = conn.get_bucket('bucket-name')
# >>> k = Key(bucket)
# >>> k.key = 'filename.txt'
# >>> k.open()
# >>> k.read(10)


# my_bucket = s3.Bucket('taohonginsight18b')

# print my_bucket

# import boto
# c = boto.connect_s3()
# bucket = c.lookup('garnaat_pub')
# key = bucket.lookup('Scan1.jpg')
# for bytes in key:
  # write bytes to output stream

# def upload_file_to_s3(filename, key_dict, acl="public-read"):
#     try:
#         s3 = boto3.client(
#             "s3",
#             aws_access_key_id = key_dict['S3_KEY'],
#             aws_secret_access_key = key_dict['S3_SECRET']
#         )

#         name_temp = id_generator() + ".csv"
#         print 'uploading ' + str(filename) + ' to ' + key_dict['S3_BUCKET'] + " as " + name_temp

#         with open(filename, 'rb') as data:
#             s3.upload_fileobj(
#                 data,
#                 key_dict['S3_BUCKET'],
#                 name_temp,
#                 ExtraArgs={
#                     "ACL": acl,
#                     "ContentType": "application/csv",
#                 }
#             )
#         return "{}{}".format(key_dict['S3_LOCATION'], name_temp)
#     except Exception as e:
#         # This is a catch all exception, edit this part to fit your needs.
#         print("Something Happened: ", e)
#         name_ID = id_generator()
#         wf = os.path.dirname(filename)
#         os.rename(filename, wf + "/"+name_ID+".csv")
#         return name_ID





# conn = boto.connect_s3(S3_KEY, S3_SECRET)
# bucket = conn.get_bucket(S3_BUCKET)


# fileobj = s3.Object('taohonginsight18b', 'UnderlyingOptionsTradesCalcs_2018-02.csv').get()['Body']

# for idx in range(5):
#     chunk = fileobj.read(1)
#     if idx == 1:
#         print chunk
#     else:
#         print chunk




# for file in bucket.list():
#     print type(file)
#     unfinished_line = ''
#     for byte in file:
#         # print byte
#         byte = unfinished_line + byte
#         lines = byte.split('\n')
#         # print type(lines)
#         unfinished_line = lines.pop()
#         for line in lines[:3]:
#             print line, "====="
#             print "\n"