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
  "net/url"

  "github.com/Bowbaq/profilecreds"
  "github.com/davecgh/go-spew/spew"

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
  Bucket       string
  Key          string
  Encryption   string
  Size         int64
  LastModified time.Time
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
    DiskLogger.WithFields(logrus.Fields{
      "function": "getMetadataWorker",
      "num":      num,
      "object":   object,
    }).Debug("calling getMetaDataForObject()")

    wg.Add(1)
    go func() {
      results <- getMetaDataForObject(object)
    }()
  }
}

func encryptObjectWorker(num int, jobs <-chan *s3Object, results chan<- *s3Object) {
  for object := range jobs {
    DiskLogger.WithFields(logrus.Fields{
      "function": "encryptObjectWorker",
      "num":      num,
      "object":   object,
    }).Debug("calling encryptObject()")

    wg.Add(1)
    go func() {
      results <- encryptObject(object)
    }()
  }
}

func encryptObjectInBucket(bucketName string, key string) {
  object := getObjectForBucket(bucketName, key)
  spew.Dump(object)
  if object.Encryption != neededEncryptionType {
    spew.Dump(encryptObject(object))
  }
}

func encryptObject(object *s3Object) *s3Object {
  copySource := fmt.Sprintf("%s/%s", object.Bucket, object.Key)

  params := &s3.CopyObjectInput{
    Bucket:               aws.String(object.Bucket),
    Key:                  aws.String(object.Key),
    CopySource:           aws.String(url.QueryEscape(copySource)),
    ServerSideEncryption: aws.String(neededEncryptionType),
  }

  _, err := s3Client.CopyObject(params)

  if err != nil {
    Logger.WithFields(logrus.Fields{
      "function":        "encryptObject",
      "called_function": "s3Client.CopyObject(params)",
      "params":          params,
      "object":          object,
    }).Error(err.Error())
  } else {
    return getObjectForBucket(object.Bucket, object.Key)
  }

  return nil
}

func getObjectForBucket(bucketName string, key string) *s3Object {
  params := &s3.GetObjectInput{
    Bucket: aws.String(bucketName),
    Key:    aws.String(key),
  }

  tmpObject, err := s3Client.GetObject(params)
  if err != nil {
    Logger.WithFields(logrus.Fields{
      "function":        "getObject",
      "called_function": "s3Client.GetObject(params)",
      "params":          params,
      "bucketName":      bucketName,
      "key":             key,
    }).Error(err.Error())
  } else {
    object := &s3Object{Bucket: bucketName, Key: key, Encryption: "????", Size: *tmpObject.ContentLength, LastModified: *tmpObject.LastModified}
    return getMetaDataForObject(object)
  }

  return nil
}

func getMetaDataForObject(object *s3Object) *s3Object {
  params := &s3.HeadObjectInput{
    Bucket: aws.String(object.Bucket),
    Key:    aws.String(object.Key),
  }

  tmpObject, err := s3Client.HeadObject(params)
  if err != nil {
    Logger.WithFields(logrus.Fields{
      "function":        "getMetaDataForObject",
      "called_function": "s3Client.HeadObject(params)",
      "params":          params,
      "object":          object,
    }).Error(err.Error())
  } else {
    if tmpObject.ServerSideEncryption != nil {
      object.Encryption = *tmpObject.ServerSideEncryption
    } else {
      object.Encryption = "NONE"
    }
    return object
  }

  return nil
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
    Logger.WithFields(logrus.Fields{
      "function":        "getObjectsForBucket",
      "called_function": "s3Client.ListObjectsV2(listObjectsParams)",
      "params":          listObjectsParams,
    }).Error(err.Error())
    return
  }

  jobCount := 0

  for _, tmpObject := range resp.Contents {
    jobCount++
    object := &s3Object{Bucket: bucketName, Key: *tmpObject.Key, Encryption: "????", Size: *tmpObject.Size, LastModified: *tmpObject.LastModified}
    DiskLogger.WithFields(logrus.Fields{
      "function": "getObjectsForBucket",
      "object":   object,
    }).Debug()
    jobs <- object
  }

  if *resp.IsTruncated {
    continuationToken = *resp.NextContinuationToken
    startAfter = *resp.Contents[jobCount-1].Key

    getObjectsForBucket(jobs, results, bucketName, continuationToken, startAfter)
  }
}

func reportOnObjectInBucket(bucketName string, key string) {
  spew.Dump(getObjectForBucket(bucketName, key))
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
        Logger.Info("Found " + strconv.Itoa(count) + " objects so far..")
      }
      DiskLogger.WithFields(logrus.Fields{
        "encryption": object.Encryption,
        "updated":    object.LastModified,
        "size":       object.Size,
      }).Info(object.Key)
      wg.Done()
    }
    Logger.Info("Found " + strconv.Itoa(count) + " objects total")
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

func logObjectToDisk(object *s3Object, changed bool) {
  DiskLogger.WithFields(logrus.Fields{
    "encryption": object.Encryption,
    "updated":    object.LastModified,
    "size":       object.Size,
    "changed":    changed,
  }).Info(object.Key)
}

func encryptObjectsInBucket(bucketName string) {
  Logger.Info("Getting list of objects..")

  getObjectsForBucketJobs := make(chan *s3Object, 1000)
  getObjectsForBucketResults := make(chan *s3Object, 1000)

  encryptObjectWorkerJobs := make(chan *s3Object)
  encryptObjectWorkerResults := make(chan *s3Object)

  go func() {
    count := 0
    for object := range getObjectsForBucketResults {
      count++
      if (count % 5000) == 0 {
        Logger.Info("Found " + strconv.Itoa(count) + " objects so far..")
      }
      if object.Encryption == neededEncryptionType {
        logObjectToDisk(object, false)
      } else {
        encryptObjectWorkerJobs <- object
      }
      wg.Done()
    }
    Logger.Info("Found " + strconv.Itoa(count) + " objects total")
  }()

  go func() {
    count := 0
    for object := range encryptObjectWorkerResults {
      count++
      if (count % 5000) == 0 {
        Logger.Info("Encrypted " + strconv.Itoa(count) + " objects so far..")
      }
      logObjectToDisk(object, true)
      wg.Done()
    }
    Logger.Info("Encrypted " + strconv.Itoa(count) + " objects total")
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
    return cli.NewExitError("ERROR: AWS S3 bucket name is empty", 2)
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

func setupProfiler() {
  if DEBUG {
    go func() {
      log.Println(http.ListenAndServe("localhost:6060", nil))
    }()
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
  setupProfiler()

  numCPUs := runtime.NumCPU()
  runtime.GOMAXPROCS(numCPUs)

  return awsBucketName, nil
}

func main() {
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
        key := c.Args().Get(0)
        awsBucketName, err := setup(c, "report")
        if err != nil {
          return err
        }

        if len(key) == 0 {
          reportOnBucket(awsBucketName)
        } else {
          reportOnObjectInBucket(awsBucketName, key)
        }
        return nil
      },
    },
    {
      Name:        "encrypt",
      Usage:       "Encrypt",
      Description: "Encrypt every object in the bucket",
      Action: func(c *cli.Context) error {
        key := c.Args().Get(0)
        awsBucketName, err := setup(c, "encrypt")
        if err != nil {
          return err
        }

        if len(key) == 0 {
          encryptObjectsInBucket(awsBucketName)
        } else {
          encryptObjectInBucket(awsBucketName, key)
        }
        return nil
      },
    },
  }

  app.Run(os.Args)
}
