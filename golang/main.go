package main

import (
    "context"
    "encoding/json"
    "fmt"
    "github.com/aws/aws-lambda-go/events"
    "github.com/aws/aws-lambda-go/lambda"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/sns"
    "github.com/google/uuid"
)

var (
    dynamoDBSvc *dynamodb.DynamoDB
    snsSvc      *sns.SNS
)

func init() {
    // Initialize AWS session and clients for DynamoDB and SNS
    sess := session.Must(session.NewSession())
    dynamoDBSvc = dynamodb.New(sess)
    snsSvc = sns.New(sess)
}

// Lambda handler
func lambdaHandler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
    text := request.Body
    postId := uuid.New().String()

    // Store metadata in DynamoDB
    _, err := dynamoDBSvc.PutItem(&dynamodb.PutItemInput{
        TableName: aws.String("Posts"),
        Item: map[string]*dynamodb.AttributeValue{
            "PostId": {
                S: aws.String(postId),
            },
            "Status": {
                S: aws.String("Processing"),
            },
            "Text": {
                S: aws.String(text),
            },
        },
    })
    if err != nil {
        return events.APIGatewayProxyResponse{StatusCode: 500, Body: fmt.Sprintf("Error storing item: %v", err)}, nil
    }

    // Publish message to SNS
    message, _ := json.Marshal(map[string]string{
        "PostId": postId,
    })
    _, err = snsSvc.Publish(&sns.PublishInput{
        TopicArn: aws.String("arn:aws:sns:us-east-1:123456789012:TTSNotifications"),
        Message:  aws.String(string(message)),
        Subject:  aws.String("New Post for TTS"),
    })
    if err != nil {
        return events.APIGatewayProxyResponse{StatusCode: 500, Body: fmt.Sprintf("Error publishing message: %v", err)}, nil
    }

    // Response
    responseBody, _ := json.Marshal(map[string]string{
        "message": "Post received",
        "PostId":  postId,
    })

    return events.APIGatewayProxyResponse{
        StatusCode: 200,
        Body:       string(responseBody),
    }, nil
}

func main() {
    lambda.Start(lambdaHandler)
}
