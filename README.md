# de-stash-dump2s3.py

An utility to dump a database and sync it to AWS S3 bucket. It keeps only certain amount of dumps and removes the oldest ones. \
Consider today is 2024-07-29, after dump is complete the script will delete all dumps except the following: \
Last 7 days dumps: \
- 2024-07-29 
- 2024-07-28 
- 2024-07-27 
- 2024-07-26 
- 2024-07-25 
- 2024-07-24 
- 2024-07-23 

Last 4 weeks dumps (1,8,15,22 days of month): \
- 2024-07-22 
- 2024-07-15 
- 2024-07-08 
- 2024-07-01 

Last 3 months dumps (1st day of month): \
- 2024-06-01 
- 2024-05-01 
- 2024-04-01 
