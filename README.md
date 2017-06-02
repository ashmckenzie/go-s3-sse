# go-s3-sse

Ensure every object in a given S3 bucket is encrypted

## How it works

Ensuring an S3 bucket's contents is encryped can be painful as AWS does not make it easy.  This tool aims to address this.

## Help

```
NAME:
   s3-sse - S3 SSE tool

USAGE:
   s3_sse [global options] command [command options] [arguments...]

VERSION:
   0.2.2

COMMANDS:
     report   Report
     encrypt  Encrypt
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --quiet                        Quiet mode [$QUIET]
   --debug                        Debug mode [$DEBUG]
   --workers value                Worker count (default: 32) [$WORKER_COUNT]
   --log-file value, -l value     Log file name [$LOG_FILE]
   --credentials value, -c value  AWS credentials location [$CREDENTIALS]
   --role value                   AWS IAM role [$AWS_ROLE]
   --region value                 AWS region [$AWS_REGION]
   --bucket value                 AWS S3 bucket [$AWS_BUCKET]
   --help, -h                     show help
   --version, -v                  print the version
 ```
