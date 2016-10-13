package main

import (
  "fmt"
  "log"
  "os"
  "runtime"
  "strconv"
  "sync"
  "time"

  "net/http"
  _ "net/http/pprof"

  "github.com/Bowbaq/profilecreds"

  "github.com/Sirupsen/logrus"

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

var wg sync.WaitGroup

const neededEncryptionType string = "AES256"

type s3Object struct {
  Bucket     string
  Key        string
  Encryption string
}

func setupS3Client(awsRegionName string, credentialsFileName string, roleName string) {
  config := aws.Config{
    S3ForcePathStyle: aws.Bool(false),
    Region:           aws.String(awsRegionName),
  }

  if len(roleName) > 0 {
    profilecreds.DefaultDuration = 1 * time.Hour
    creds := profilecreds.NewCredentials(roleName, func(p *profilecreds.AssumeRoleProfileProvider) {
      p.Cache = profilecreds.NewFileCache("")
    })
    config.Credentials = creds
  }

  s3Client = s3.New(session.New(), &config)
}

func getMetadataWorker(num int, jobs <-chan *s3Object, results chan<- *s3Object) {
  for object := range jobs {
    DiskLogger.Debug(fmt.Sprintf("getMetadataWorker(): num=[%d], object=[%v] - calling getMetaDataForObject()", num, object))
    wg.Add(1)
    go getMetaDataForObject(object, results)
  }
}

func encryptObjectWorker(num int, jobs <-chan *s3Object, results chan<- *s3Object) {
  for object := range jobs {
    DiskLogger.Debug(fmt.Sprintf("encryptObjectWorker(): num=[%d], object=[%v] - calling encryptObject()", num, object))
    wg.Add(1)
    go encryptObject(object, results)
  }
}

func encryptObject(object *s3Object, results chan<- *s3Object) {
  copySource := fmt.Sprintf("%s/%s", object.Bucket, object.Key)

  params := &s3.CopyObjectInput{
    Bucket:               aws.String(object.Bucket),
    Key:                  aws.String(object.Key),
    CopySource:           aws.String(copySource),
    ServerSideEncryption: aws.String(neededEncryptionType),
  }

  _, err := s3Client.CopyObject(params)

  if err != nil {
    Logger.Error("encryptObjectWorker(): s3Client.CopyObject(params) " + err.Error())
  } else {
    object.Encryption = neededEncryptionType
  }

  results <- object
}

func getMetaDataForObject(object *s3Object, results chan<- *s3Object) {
  params := &s3.HeadObjectInput{
    Bucket: aws.String(object.Bucket),
    Key:    aws.String(object.Key),
  }

  head, err := s3Client.HeadObject(params)
  if err != nil {
    Logger.Error("getMetaDataForObject(): s3Client.HeadObject(params) " + err.Error())
  } else {
    if head.ServerSideEncryption != nil {
      object.Encryption = *head.ServerSideEncryption
    } else {
      object.Encryption = "UNKNOWN"
    }
  }

  results <- object
}

func getObjectsForBucket(jobs chan<- *s3Object, results chan<- *s3Object, bucketName string, continuationToken string, startAfter string) {
  listObjectsParams := &s3.ListObjectsV2Input{
    Bucket: aws.String(bucketName),
  }

  if len(continuationToken) != 0 && len(startAfter) != 0 {
    listObjectsParams.ContinuationToken = aws.String(continuationToken)
    listObjectsParams.StartAfter = aws.String(startAfter)
  }

  resp, err := s3Client.ListObjectsV2(listObjectsParams)

  if err != nil {
    Logger.Error("getObjectsForBucket(): s3Client.ListObjectsV2(listObjectsParams) " + err.Error())
  }

  jobCount := 0

  for _, tmpObject := range resp.Contents {
    jobCount++
    object := &s3Object{Bucket: bucketName, Key: *tmpObject.Key, Encryption: "????"}
    DiskLogger.Debug(fmt.Sprintf("getObjectsForBucket(): object=[%v]", object))
    // wg.Add(1)
    jobs <- object
  }

  if *resp.IsTruncated {
    continuationToken = *resp.NextContinuationToken
    startAfter = *resp.Contents[jobCount-1].Key

    getObjectsForBucket(jobs, results, bucketName, continuationToken, startAfter)
  }
}

func reportOnBucket(bucketName string) {
  Logger.Info("Getting list of objects..")

  getObjectsForBucketJobs := make(chan *s3Object)
  getObjectsForBucketResults := make(chan *s3Object)

  go func() {
    count := 0
    for object := range getObjectsForBucketResults {
      count++
      if (count % 5000) == 0 {
        Logger.Info("Found " + strconv.Itoa(count) + " objects..")
      }
      DiskLogger.Info(string(object.Key) + " encryption:" + string(object.Encryption))
      wg.Done()
    }
  }()

  for i := 1; i <= workerCount; i++ {
    go getMetadataWorker(i, getObjectsForBucketJobs, getObjectsForBucketResults)
  }

  wg.Add(1)
  getObjectsForBucket(getObjectsForBucketJobs, getObjectsForBucketResults, bucketName, "", "")
  wg.Done()

  wg.Wait()
  close(getObjectsForBucketResults)
}

func encryptBucket(bucketName string) {
  Logger.Info("Getting list of objects..")

  getObjectsForBucketJobs := make(chan *s3Object)
  getObjectsForBucketResults := make(chan *s3Object)

  encryptObjectWorkerJobs := make(chan *s3Object)
  encryptObjectWorkerResults := make(chan *s3Object)

  go func() {
    count := 0
    for object := range getObjectsForBucketResults {
      count++
      if (count % 5000) == 0 {
        Logger.Info("Found " + strconv.Itoa(count) + " objects..")
      }
      if object.Encryption == neededEncryptionType {
        DiskLogger.Info(string(object.Key) + " encryption:" + string(object.Encryption) + " updated=false")
      } else {
        encryptObjectWorkerJobs <- object
      }
      wg.Done()
    }
  }()

  go func() {
    count := 0
    for object := range encryptObjectWorkerResults {
      count++
      if (count % 5000) == 0 {
        Logger.Info("Encrypted " + strconv.Itoa(count) + " objects..")
      }
      DiskLogger.Info(string(object.Key) + " encryption:" + string(object.Encryption) + " updated=true")
      wg.Done()
    }
  }()

  for i := 1; i <= workerCount; i++ {
    go getMetadataWorker(i, getObjectsForBucketJobs, getObjectsForBucketResults)
  }

  for i := 1; i <= workerCount; i++ {
    go encryptObjectWorker(i, encryptObjectWorkerJobs, encryptObjectWorkerResults)
  }

  wg.Add(1)
  getObjectsForBucket(getObjectsForBucketJobs, getObjectsForBucketResults, bucketName, "", "")
  wg.Done()

  wg.Wait()
  close(getObjectsForBucketResults)
  close(encryptObjectWorkerResults)
}

func validateParams(regionName string, bucketName string, roleName string) error {
  if len(regionName) == 0 {
    return cli.NewExitError("ERROR: AWS region name is empty", 1)
  }

  if len(bucketName) == 0 {
    return cli.NewExitError("ERROR: AWS S3 bucket name is empty", 1)
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

  Logger.Level = logrus.WarnLevel
  DiskLogger.Level = logrus.InfoLevel

  if DEBUG {
    Logger.Level = logrus.DebugLevel
    DiskLogger.Level = logrus.DebugLevel
  } else if VERBOSE {
    Logger.Level = logrus.InfoLevel
  }
}

func setup(c *cli.Context, style string) (string, error) {
  logFileName := c.GlobalString("log-file")
  credentialsFileName := c.GlobalString("credentials")
  awsRoleName := c.GlobalString("role")
  awsRegionName := c.GlobalString("region")
  awsBucketName := c.GlobalString("bucket")

  err := validateParams(awsRegionName, awsBucketName, awsRoleName)
  if err != nil {
    return "", err
  }

  if len(logFileName) == 0 {
    logFileName = fmt.Sprintf("%s.log", awsBucketName)
  }

  setupLogging(logFileName, style)
  setupS3Client(awsRegionName, credentialsFileName, awsRoleName)

  numCPUs := runtime.NumCPU()
  runtime.GOMAXPROCS(numCPUs)

  return awsBucketName, nil
}

func main() {
  go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
  }()

  app := cli.NewApp()

  app.Name = "s3-sse"
  app.Usage = "S3 SSE tool"
  app.Version = os.Getenv("VERSION")

  app.Flags = []cli.Flag{
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
    cli.IntFlag{
      Name:        "workers",
      Usage:       "Worker count",
      EnvVar:      "WORKER_COUNT",
      Value:       runtime.NumCPU() * 8,
      Destination: &workerCount,
    },
    cli.StringFlag{
      Name:   "log-file, l",
      Usage:  "Log file name",
      EnvVar: "LOG_FILE",
    },
    cli.StringFlag{
      Name:   "credentials, c",
      Usage:  "AWS credentials location",
      EnvVar: "CREDENTIALS",
    },
    cli.StringFlag{
      Name:   "role",
      Usage:  "AWS IAM role",
      EnvVar: "AWS_ROLE",
    },
    cli.StringFlag{
      Name:   "region",
      Usage:  "AWS region",
      EnvVar: "AWS_REGION",
    },
    cli.StringFlag{
      Name:   "bucket",
      Usage:  "AWS S3 bucket",
      EnvVar: "AWS_BUCKET",
    },
  }

  app.Commands = []cli.Command{
    {
      Name:        "report",
      Usage:       "Report",
      Description: "Provide a report of the encryption status of each object",
      Action: func(c *cli.Context) error {
        awsBucketName, err := setup(c, "report")
        if err != nil {
          return err
        }

        reportOnBucket(awsBucketName)
        return nil
      },
    },
    {
      Name:        "encrypt",
      Usage:       "Encrypt",
      Description: "Encrypt every object in the bucket",
      Action: func(c *cli.Context) error {
        awsBucketName, err := setup(c, "encrypt")
        if err != nil {
          return err
        }

        encryptBucket(awsBucketName)
        return nil
      },
    },
  }

  app.Run(os.Args)
}
