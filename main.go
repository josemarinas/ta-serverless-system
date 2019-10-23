package main

import(
	"strings"
	"bytes"
	"fmt"
	"os"
  "context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)
var outputQueue string
var bucket string
var sess *session.Session
var sqsService *sqs.SQS
var s3Service *s3.S3
var uploader *s3manager.Uploader
var downloader *s3manager.Downloader
func init() {
	var region = os.Getenv("AWS_REGION")
	outputQueue = os.Getenv("AWS_OUTPUT_QUEUE")
	bucket = os.Getenv("AWS_BUCKET")
	sess = session.New(&aws.Config{Region: &region,})
	sqsService = sqs.New(sess)
	s3Service = s3.New(sess)
	uploader = s3manager.NewUploader(sess)
	downloader = s3manager.NewDownloader(sess)
}
func main() {
	lambda.Start(handleInputMessage)
}

func handleInputMessage(ctx context.Context, sqsEvent events.SQSEvent) error {
	var err error
	for _, message := range sqsEvent.Records {
		if (*message.MessageAttributes["Command"].StringValue == "echo") {
			err = sendMessage(
				&message.Body,
				message.MessageAttributes["User"].StringValue,
				message.MessageAttributes["Session"].StringValue,
				&outputQueue,
				aws.String("echo"),
			)
			if err != nil {
				return err
			}
			err = appendToS3Object(
				&message.Body,
				message.MessageAttributes["User"].StringValue,
				message.MessageAttributes["Session"].StringValue,
				message.Attributes["SentTimestamp"],
			)
			if err != nil {
				return err
			}
		} else if (*message.MessageAttributes["Command"].StringValue == "search"){
			found, err := searchMessages(
				&message.Body,
				message.MessageAttributes["User"].StringValue,
			)
			if err != nil {
				return err
			}
			if(found == ""){
				found = "Not Found"
			}
			err = sendMessage(
				&found,
				message.MessageAttributes["User"].StringValue,
				message.MessageAttributes["Session"].StringValue,
				&outputQueue,
				aws.String("search"),
			)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("Command not supported")
		}
	}
	return  nil
}

func sendMessage(message *string, user *string, session *string, queue *string, command *string) error {
	_, err := sqsService.SendMessage(&sqs.SendMessageInput{
		QueueUrl:            	queue,
		MessageBody:					message,
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"User": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: user,
			},
			"Command": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: command,
			},
			"Session": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: session,
			},
		},
	})
	if err != nil {
		return err
	} 
	return nil
}

func appendToS3Object(message *string, user *string, session *string, timestamp string) error {
	buff := &aws.WriteAtBuffer{}
	_, _ = downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("/%s/%s.txt", *user, *session)),
	})
	s3Conversation := fmt.Sprintf("%s\n%s\n%s\n%s\n", string(buff.Bytes()), timestamp, fmt.Sprintf("User: %s", *message), fmt.Sprintf("Echo: %s", *message))
	fmt.Println(s3Conversation)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:   bytes.NewReader([]byte(s3Conversation)),
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf("/%s/%s.txt", *user, *session)),
	})
	if err != nil {
		return err
	}
	return nil
}

func searchMessages(message *string, user *string)(string, error) {
	var ret string
	params := &s3.ListObjectsInput {
    Bucket: aws.String(bucket),
    Prefix: aws.String(fmt.Sprintf("%s", *user)),
	}
	resp, _ := s3Service.ListObjects(params)
	for _, object := range resp.Contents {
		buff := &aws.WriteAtBuffer{}
		_, err := downloader.Download(buff, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    object.Key,
		})
		if err != nil {
			return "", err
		}
		buffString := string(buff.Bytes())
		splitBreak := func(c rune) bool {
			return c == '\n'
		}
		splitColon:= func(c rune) bool {
			return c == ':'
		}
		for _, str := range strings.FieldsFunc(buffString, splitBreak) {
			if (len(strings.FieldsFunc(str, splitColon)) > 1) {
				if(strings.Contains(strings.FieldsFunc(str, splitColon)[1], *message)) {
					ret = fmt.Sprintf("%s\n%s", ret, str)
				}
			}
		}
	}
	return ret, nil
}
