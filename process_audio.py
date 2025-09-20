# import boto3


# transcribe = boto3.client('transcribe')
# translate = boto3.client('translate')
# polly = boto3.client('polly')
# s3 = boto3.client('s3')

# response = transcribe.start_transcription_job(
#     TranscriptionJobName='string',
#     LanguageCode='en-US',
#     MediaFormat='mp3',
#     Media={
#         'MediaFileUri': 'string',
#         'RedactedMediaFileUri': 'string'
#     },
#     OutputBucketName='string',
#     OutputKey='string',
#     Tags=[
#         {
#             'Key': 'string',
#             'Value': 'string'
#         },
#     ]

# )