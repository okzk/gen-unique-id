package main

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
	"github.com/okzk/ticker"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type Config struct {
	Table string `required:"true"`
	MinID int    `split_words:"true" default:"0"`
	MaxID int    `split_words:"true" default:"1023"`
	TTL   int    `default:"600"`
}

var conf Config

func main() {
	if err := envconfig.Process("", &conf); err != nil {
		log.Fatalf("[FATAL] %v", err)
	}

	svc := dynamodb.New(session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})))

	u := uuid.New()
	log.Printf("[INFO] uuid: %v", u)
	bID := u[:]

	nID, err := obtainID(svc, bID)
	if err != nil {
		log.Fatalf("[FATAL] %v", err)
	}
	log.Printf("[INFO] id: %d", nID)

	tick := ticker.New(time.Duration(conf.TTL)*time.Second*2/5, func(_ time.Time) {
		updateTTL(svc, nID, bID)
	})
	defer tick.Stop()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "test/plain")
		fmt.Fprintf(w, "%d", nID)
	})
	http.ListenAndServe(":8000", nil)
}

func obtainID(svc *dynamodb.DynamoDB, bID []byte) (int, error) {
	res, err := svc.Scan(&dynamodb.ScanInput{
		AttributesToGet: []*string{aws.String("i")},
		TableName:       aws.String(conf.Table),
	})
	if err != nil {
		return 0, err
	}

	existsIDs := make(map[int]bool)
	for _, item := range res.Items {
		nID, err := strconv.Atoi(aws.StringValue(item["i"].N))
		if err != nil {
			existsIDs[nID] = true
		}
	}

	for _, n := range permutation(conf.MaxID - conf.MinID + 1) {
		nID := conf.MinID + n
		if existsIDs[nID] {
			continue
		}

		ok, err := put(svc, nID, bID)
		if err != nil {
			return 0, err
		}
		if ok {
			return nID, nil
		}
	}

	return 0, errors.New("no available ID")
}

func put(svc *dynamodb.DynamoDB, nID int, bID []byte) (bool, error) {
	t := int(time.Now().Add(time.Duration(conf.TTL) * time.Second).Unix())
	_, err := svc.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"i": {N: aws.String(strconv.Itoa(nID))},
			"b": {B: bID},
			"t": {N: aws.String(strconv.Itoa(t))},
		},
		ConditionExpression: aws.String("attribute_not_exists(i)"),
		TableName:           aws.String(conf.Table),
	})
	if err != nil {
		if e, ok := err.(awserr.Error); ok {
			if e.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func updateTTL(svc *dynamodb.DynamoDB, nID int, bID []byte) {
	log.Println("[INFO] updating TTL...")

	t := int(time.Now().Add(time.Duration(conf.TTL) * time.Second).Unix())
	_, err := svc.UpdateItem(&dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"i": {N: aws.String(strconv.Itoa(nID))},
		},
		UpdateExpression:    aws.String("set t = :t"),
		ConditionExpression: aws.String("b = :b"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":b": {B: bID},
			":t": {N: aws.String(strconv.Itoa(t))},
		},
		TableName: aws.String(conf.Table),
	})
	if err != nil {
		if e, ok := err.(awserr.Error); ok {
			switch e.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException, dynamodb.ErrCodeInternalServerError:
				log.Printf("[WARN] fail to update TTL temporally: %v", err)
				return
			}
		}
		log.Fatalf("[FATAL] aborting: %v", err)
	}
}

func permutation(n int) []int {
	t := time.Now()
	s := rand.NewSource(t.UnixNano())
	return rand.New(s).Perm(n)
}
