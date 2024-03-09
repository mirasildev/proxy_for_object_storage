package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	awsCredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	minioCredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/viper"
)

type ObjectStorageConfig struct {
	AccessKey string
	SecretKey string
	Endpoint  string
}

type Config struct {
	MinioConfig ObjectStorageConfig
	AWSConfig   ObjectStorageConfig
	ServiceName string
}

var globalConfig Config

func loadConfig(path string) error {
	godotenv.Load(path + "/.env") // load .env file if it exists

	conf := viper.New()
	conf.AutomaticEnv()

	globalConfig = Config{
		MinioConfig: ObjectStorageConfig{
			AccessKey: conf.GetString("MINIO_ACCESS_KEY"),
			SecretKey: conf.GetString("MINIO_SECRET_KEY"),
			Endpoint:  conf.GetString("MINIO_ENDPOINT"),
		},
		AWSConfig: ObjectStorageConfig{
			AccessKey: conf.GetString("AWS_ACCESS_KEY"),
			SecretKey: conf.GetString("AWS_SECRET_KEY"),
			Endpoint:  conf.GetString("AWS_ENDPOINT"),
		},
		ServiceName: conf.GetString("SERVICE_NAME"),
	}

	return nil
}

type Presigner struct {
	PresignClient *s3.PresignClient
}

func (presigner Presigner) GetObject(bucketName string, objectKey string, lifetimeSecs int64) (*v4.PresignedHTTPRequest, error) {
	fmt.Println("Getting a presigned request to get object:", bucketName, objectKey)
	request, err := presigner.PresignClient.PresignGetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = time.Duration(lifetimeSecs * int64(time.Second))
	})
	if err != nil {
		log.Printf("Couldn't get a presigned request to get %v:%v. Here's why: %v\n",
			bucketName, objectKey, err)
	}
	return request, err
}

func getPresignedURL(bucketName, fileName string, lifetimeSecs int64) (string, error) {
	if globalConfig.ServiceName == "minio" {
		// Initialize Minio client
		minioClient, err := minio.New(globalConfig.MinioConfig.Endpoint, &minio.Options{
			Creds:  minioCredentials.NewStaticV4(globalConfig.MinioConfig.AccessKey, globalConfig.MinioConfig.SecretKey, ""),
			Secure: true,
			Region: "auto",
		})
		if err != nil {
			return "", err
		}

		// Generates a presigned url which expires in a day.
		respURL, err := minioClient.PresignedGetObject(context.Background(), bucketName, fileName, time.Second*60, nil)
		if err != nil {
			return "", err
		}

		return respURL.String(), nil
	} else if globalConfig.ServiceName == "S3" || globalConfig.ServiceName == "R2" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: globalConfig.AWSConfig.Endpoint,
			}, nil
		})

		awsCfg, err := awsConfig.LoadDefaultConfig(context.TODO(),
			awsConfig.WithEndpointResolverWithOptions(resolver),
			awsConfig.WithCredentialsProvider(awsCredentials.NewStaticCredentialsProvider(globalConfig.AWSConfig.AccessKey, globalConfig.AWSConfig.SecretKey, "")),
			awsConfig.WithRegion("auto"),
		)
		if err != nil {
			return "", err
		}

		s3Client := s3.NewFromConfig(awsCfg)
		presignClient := s3.NewPresignClient(s3Client)
		presigner := Presigner{PresignClient: presignClient}

		respURL, err := presigner.GetObject(bucketName, fileName, lifetimeSecs)
		if err != nil {
			return "", err
		}

		return respURL.URL, nil
	}

	return "", errors.New("unsupported object storage service")
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	var (
		// Extract bucket name and file name
		path         = strings.TrimPrefix(r.URL.Path, "/stream/")
		bucketName   = extractBucketName(path)
		fileName     = extractFileName(path)
		presignedURL string
	)
	fmt.Println("globalConfig:::::", globalConfig)
	fmt.Println("path:::::", path)
	fmt.Println("bucketName:::::", bucketName)
	fmt.Println("fileName:::::", fileName)

	presignedURL, err := getPresignedURL(bucketName, fileName, 10)
	if err != nil {
		fmt.Fprintf(w, "Error getting presigned URL: %v", err)
		return
	}

	log.Printf("Got a presigned %v request to URL:\n\t%v\n", presignedURL,
		presignedURL)
	log.Println("Using net/http to send the request...")

	getResponse, err := http.Get(presignedURL)
	if err != nil {
		fmt.Fprintf(w, "Error getting object: %v", err)
		return
	}

	defer getResponse.Body.Close()

	buffer := bytes.NewBuffer(nil)
	_, err = io.Copy(buffer, getResponse.Body)
	if err != nil {
		fmt.Fprintf(w, "Error reading object: %v", err)
		return
	}

	// Set the Content-Type header explicitly
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", fmt.Sprintf("%v", buffer.Len()))
	w.Header().Set("Access-Control-Allow-Origin", "*")

	http.ServeContent(w, r, fileName, time.Time{}, bytes.NewReader(buffer.Bytes()))
}

func extractBucketName(path string) string {
	// Split the path into segments
	segments := strings.Split(path, "/")

	// Bucket name is expected to be the second segment
	return segments[1]
}

func extractFileName(path string) string {
	// Split the path into segments
	segments := strings.Split(path, "/")

	// Bucket name is expected to be in the third segment and after
	return strings.Join(segments[2:], "/")
}

func main() {
	// Load configuration
	if err := loadConfig("."); err != nil {
		log.Fatal(err)
	}

	// Start server
	http.HandleFunc("/stream/", handleRequest)
	fmt.Println("Listening on :8000")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
