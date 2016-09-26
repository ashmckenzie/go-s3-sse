package main

import (
  "fmt"
  "log"
  "os"
  "runtime/debug"

  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/credentials"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/s3"
  "github.com/urfave/cli"
)

// DEBUG ...
var DEBUG = false

type s3Object struct {
  Bucket     string
  Key        string
  Encryption string
}

func errorPrint(err string) {
  log.Printf(err)
  if DEBUG {
    panic(debug.Stack())
  }
}

func s3Client(credentialsFileName string, roleName string) *s3.S3 {
  creds := credentials.NewSharedCredentials(credentialsFileName, roleName)

  config := aws.Config{
    Credentials: creds,
    Region:      aws.String("us-east-1"),
  }

  return s3.New(session.New(), &config)
}

func objectsFor(s3Client *s3.S3, bucketName string) []*s3Object {
  var objects []*s3Object

  listObjectsParams := &s3.ListObjectsV2Input{
    Bucket: aws.String(bucketName),
  }

  resp, err := s3Client.ListObjectsV2(listObjectsParams)
  if err != nil {
    errorPrint("objectsFor(): s3Client.ListObjectsV2(listObjectsParams) " + err.Error())
  }

  for _, tmpObject := range resp.Contents {
    params := &s3.HeadObjectInput{
      Bucket: aws.String(bucketName),
      Key:    aws.String(*tmpObject.Key),
    }

    head, err := s3Client.HeadObject(params)
    if err != nil {
      errorPrint("objectsFor(): s3Client.HeadObject(params) " + err.Error())
    }

    encryption := "NONE"
    if head.ServerSideEncryption != nil {
      encryption = *head.ServerSideEncryption
    }

    object := &s3Object{Bucket: bucketName, Key: *tmpObject.Key, Encryption: encryption}
    objects = append(objects, object)
  }

  return objects
}

func reportForBucket(s3Client *s3.S3, bucketName string) {
  log.Println("reportForBucket(): Getting objects..")
  objects := objectsFor(s3Client, bucketName)

  for _, object := range objects {
    log.Printf("%s encryption:%s", object.Key, object.Encryption)
  }
}

func encryptBucket(s3Client *s3.S3, bucketName string) {
  log.Println("encryptBucket(): Getting objects..")
  objects := objectsFor(s3Client, bucketName)

  for _, object := range objects {
    if object.Encryption != "AES256" {
      log.Printf("%s encryption:%s (WILL ENCRYPT)", object.Key, object.Encryption)
      encryptObject(s3Client, object)
    }
  }
}

func encryptObject(s3Client *s3.S3, object *s3Object) {
  copySource := fmt.Sprintf("%s/%s", object.Bucket, object.Key)

  params := &s3.CopyObjectInput{
    Bucket:               aws.String(object.Bucket),
    Key:                  aws.String(object.Key),
    CopySource:           aws.String(copySource),
    ServerSideEncryption: aws.String("AES256"),
  }

  _, err := s3Client.CopyObject(params)

  if err != nil {
    errorPrint("encryptObject(): s3Client.CopyObject(params) " + err.Error())
  }
}

func validateParams(bucketName string, roleName string) error {
  if len(bucketName) == 0 {
    return cli.NewExitError("ERROR: AWS S3 bucket name is empty", 1)
  }
  if len(roleName) == 0 {
    return cli.NewExitError("ERROR: AWS IAM role name is empty", 2)
  }
  return nil
}

func main() {
  var credentialsFileName string
  var bucketName string
  var roleName string

  app := cli.NewApp()
  app.Name = "s3-sse"
  app.Usage = "S3 SSE tool"
  app.Version = "0.1.0"

  app.Flags = []cli.Flag{
    cli.StringFlag{
      Name:        "credentials, c",
      Usage:       "AWS credentials location",
      EnvVar:      "CREDENTIALS_FILE_NAME",
      Destination: &credentialsFileName,
    },
    cli.StringFlag{
      Name:        "bucket, b",
      Usage:       "AWS S3 bucket",
      EnvVar:      "BUCKET_NAME",
      Destination: &bucketName,
    },
    cli.StringFlag{
      Name:        "role, r",
      Usage:       "AWS IAM role",
      EnvVar:      "ROLE_NAME",
      Destination: &roleName,
    },
    cli.BoolFlag{
      Name:        "debug",
      Usage:       "Debug mode",
      EnvVar:      "DEBUG",
      Destination: &DEBUG,
    },
  }

  app.Commands = []cli.Command{
    {
      Name:        "report",
      Usage:       "Report",
      Description: "Provide a report of the encryption status of each object",
      Action: func(c *cli.Context) error {
        err := validateParams(bucketName, roleName)
        if err != nil {
          return err
        }

        reportForBucket(s3Client(credentialsFileName, roleName), bucketName)
        return nil
      },
    },
    {
      Name:        "encrypt",
      Usage:       "Encrypt",
      Description: "Encrypt every object in the bucket",
      Action: func(c *cli.Context) error {
        err := validateParams(bucketName, roleName)
        if err != nil {
          return err
        }

        encryptBucket(s3Client(credentialsFileName, roleName), bucketName)
        return nil
      },
    },
  }

  app.Run(os.Args)
}
