package main

import (
  "fmt"
  "os"
  "runtime"
  "strconv"
  "time"

  "github.com/Sirupsen/logrus"
  _ "github.com/mattn/go-sqlite3"
  pb "gopkg.in/cheggaaa/pb.v1"

  "github.com/Bowbaq/profilecreds"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/s3"
  "github.com/urfave/cli"
)

// DEBUG ...
var DEBUG = false

// VERBOSE ...
var VERBOSE = false

var diskLoggerFile os.File

// Logger ...
var Logger *logrus.Logger

// DiskLogger ...
var DiskLogger *logrus.Logger

var s3Client *s3.S3
var workerCount int

type s3Object struct {
  Bucket     string
  Key        string
  Encryption string
}

func getS3Client(awsRegionName string, credentialsFileName string, roleName string) *s3.S3 {
  profilecreds.DefaultDuration = 1 * time.Hour

  creds := profilecreds.NewCredentials("virginia-admin", func(p *profilecreds.AssumeRoleProfileProvider) {
    p.Cache = profilecreds.NewFileCache("")
  })

  config := aws.Config{
    Credentials:      creds,
    S3ForcePathStyle: aws.Bool(false),
    Region:           aws.String(awsRegionName),
  }

  return s3.New(session.New(), &config)
}

func objectsFor(bucketName string) []*s3Object {
  continuationToken := ""
  startAfter := ""

  var objects []*s3Object

  // db, err := sql.Open("sqlite3", "./foo.db")
  // if err != nil {
  //   log.Fatal(err)
  // }
  // defer db.Close()
  //
  // sqlStmt := `
  // create table objects (id integer not null primary key, bucket text, key text, encryption text);
  // `
  // _, err = db.Exec(sqlStmt)
  // if err != nil {
  //   log.Printf("%q: %s\n", err, sqlStmt)
  // }
  // INSERT OR REPLACE INTO Employee ("id", "name", "role") VALUES (1, "John Foo", "CEO")

  Logger.Info("Getting list of objects..")

  objects = examineObjects(objects, bucketName, continuationToken, startAfter)
  objectCount := len(objects)

  if objectCount == 0 {
    Logger.Info("No objects!")
    return objects
  }

  Logger.Info("Getting metadata for " + strconv.Itoa(objectCount) + " objects..")

  jobs := make(chan *s3Object, objectCount)
  results := make(chan bool, objectCount)

  Logger.Info("Running with " + strconv.Itoa(workerCount) + " workers..")

  bar := pb.StartNew(objectCount)
  bar.Output = os.Stderr

  for i := 1; i <= workerCount; i++ {
    go getMetadataWorker(jobs, results)
  }

  for i := 0; i < objectCount; i++ {
    jobs <- objects[i]
  }
  close(jobs)

  for i := 0; i < objectCount; i++ {
    <-results
    bar.Increment()
  }
  close(results)

  bar.Finish()

  return objects
}

func getMetadataWorker(jobs <-chan *s3Object, results chan<- bool) {
  for object := range jobs {
    params := &s3.HeadObjectInput{
      Bucket: aws.String(object.Bucket),
      Key:    aws.String(object.Key),
    }

    head, err := s3Client.HeadObject(params)
    if err != nil {
      Logger.Error("objectsFor(): s3Client.HeadObject(params) " + err.Error())
    } else {
      if head.ServerSideEncryption != nil {
        object.Encryption = *head.ServerSideEncryption
      }
    }

    results <- true
  }
}

func examineObjectWorker(bucketName string, jobs <-chan *s3.Object, results chan<- *s3Object) {
  for object := range jobs {
    results <- &s3Object{Bucket: bucketName, Key: *object.Key, Encryption: "????"}
  }
}

func examineObjects(objects []*s3Object, bucketName string, continuationToken string, startAfter string) []*s3Object {
  listObjectsParams := &s3.ListObjectsV2Input{
    Bucket: aws.String(bucketName),
  }

  if len(continuationToken) != 0 && len(startAfter) != 0 {
    listObjectsParams.ContinuationToken = aws.String(continuationToken)
    listObjectsParams.StartAfter = aws.String(startAfter)
  }

  resp, err := s3Client.ListObjectsV2(listObjectsParams)

  if err != nil {
    Logger.Error("objectsFor(): s3Client.ListObjectsV2(listObjectsParams) " + err.Error())
  }

  jobCount := 0
  jobs := make(chan *s3.Object, 1000)
  results := make(chan *s3Object, 1000)

  for i := 1; i <= workerCount; i++ {
    go examineObjectWorker(bucketName, jobs, results)
  }

  for _, tmpObject := range resp.Contents {
    jobs <- tmpObject
    jobCount++
  }

  for i := 0; i < jobCount; i++ {
    objects = append(objects, <-results)
  }
  close(results)

  if *resp.IsTruncated {
    continuationToken = *resp.NextContinuationToken
    startAfter = objects[len(objects)-1].Key

    if (len(objects) % 10000) == 0 {
      Logger.Info("Found " + strconv.Itoa(len(objects)) + " objects..")
    }

    Logger.Debug("examineObjects(): continuationToken:" + continuationToken + ", startAfter:" + startAfter)

    objects = examineObjects(objects, bucketName, continuationToken, startAfter)
  }

  return objects
}

func processBucket(bucketName string, encrypt bool) {
  objects := objectsFor(bucketName)

  for _, object := range objects {
    if encrypt == true && object.Encryption != "AES256" {
      logIt(object.Key + " encryption:" + object.Encryption + " -> AES256")
      encryptObject(object)
    } else {
      logIt(object.Key + " encryption:" + object.Encryption)
    }
  }
}

func encryptObject(object *s3Object) {
  copySource := fmt.Sprintf("%s/%s", object.Bucket, object.Key)

  params := &s3.CopyObjectInput{
    Bucket:               aws.String(object.Bucket),
    Key:                  aws.String(object.Key),
    CopySource:           aws.String(copySource),
    ServerSideEncryption: aws.String("AES256"),
  }

  _, err := s3Client.CopyObject(params)

  if err != nil {
    Logger.Error("encryptObject(): s3Client.CopyObject(params) " + err.Error())
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

func setupLogging(logFileName string, extra string) {
  txtFormatter := &logrus.TextFormatter{DisableColors: true}
  fullFileName := fmt.Sprintf("%s_%s_%s", time.Now().Format("20060102_150405"), extra, logFileName)

  f, _ := os.OpenFile(fullFileName, os.O_RDWR|os.O_CREATE, 0600)

  Logger = logrus.New()
  DiskLogger = logrus.New()

  DiskLogger.Formatter = txtFormatter
  DiskLogger.Out = f

  if DEBUG {
    Logger.Level = logrus.DebugLevel
    DiskLogger.Level = logrus.DebugLevel
  } else if VERBOSE {
    Logger.Level = logrus.InfoLevel
    DiskLogger.Level = logrus.InfoLevel
  }
}

func main() {
  var awsRegionName string
  var credentialsFileName string
  var bucketName string
  var roleName string
  var logFileName string

  numCPUs := runtime.NumCPU()
  runtime.GOMAXPROCS(numCPUs)

  app := cli.NewApp()

  app.Name = "s3-sse"
  app.Usage = "S3 SSE tool"
  app.Version = os.Getenv("VERSION")

  app.Flags = []cli.Flag{
    cli.StringFlag{
      Name:        "aws-region",
      Usage:       "AWS region",
      EnvVar:      "AWS_REGION",
      Destination: &awsRegionName,
    },
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
    cli.StringFlag{
      Name:        "log-file-name, l",
      Usage:       "Log file name",
      EnvVar:      "LOG_FILE_NAME",
      Destination: &logFileName,
    },
    cli.IntFlag{
      Name:        "workers, 1",
      Usage:       "Worker count (default is number of CPU's x 16)",
      EnvVar:      "WORKER_COUNT",
      Value:       runtime.NumCPU() * 16,
      Destination: &workerCount,
    },
    cli.BoolFlag{
      Name:        "verbose",
      Usage:       "Verbose mode",
      EnvVar:      "VERBOSE",
      Destination: &VERBOSE,
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

        if len(logFileName) == 0 {
          logFileName = fmt.Sprintf("%s.log", bucketName)
        }

        setupLogging(logFileName, "report")
        s3Client = getS3Client(awsRegionName, credentialsFileName, roleName)
        processBucket(bucketName, false)
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

        if len(logFileName) == 0 {
          logFileName = fmt.Sprintf("%s.log", bucketName)
        }

        setupLogging(logFileName, "encrypt")
        s3Client = getS3Client(awsRegionName, credentialsFileName, roleName)
        processBucket(bucketName, true)
        return nil
      },
    },
  }

  app.Run(os.Args)
}
