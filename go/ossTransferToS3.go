package main

import (
	"bytes"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"net/http"
	"os"
)

type (
	Platform struct {
        OSS *Config `yaml:"oss"`
        S3 *Config `yaml:"s3"`
    }

    Config struct {
        Endpoint string
        AccessKeyID string
        AccessKeySecret string
        BucketName string
        Token string
    }
)

func ReadConfig() *Platform {
	// 读取配置
	configFile := "./config.yaml"
    yamlFile, err := ioutil.ReadFile(configFile)
    if err != nil {
        fmt.Println(err.Error())
    }
    var _config *Platform
    err = yaml.Unmarshal(yamlFile, &_config)
    if err != nil {
        fmt.Println(err.Error())
    }
	fmt.Println("read config from", configFile)
    return _config
}

func RaiseError(err error) {
	fmt.Println("Error:", err)
	os.Exit(-1)
}

func InitOss(config *Platform) *oss.Bucket {
	// 实例化oss bucket
	client, err := oss.New(config.OSS.Endpoint, config.OSS.AccessKeyID, config.OSS.AccessKeySecret)
	if err != nil {
		RaiseError(err)
	}
	bucket, err := client.Bucket(config.OSS.BucketName)
	if err != nil {
		RaiseError(err)
	}
	fmt.Println("init oss bucket success...")
	return bucket
}

func InitS3(config *Platform) *session.Session {
	// 实例化s3 bucket
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(config.S3.Endpoint),
		Credentials: credentials.NewStaticCredentials(config.S3.AccessKeyID, config.S3.AccessKeySecret, config.S3.Token),
	})
	if err != nil {
		return sess
	}
	fmt.Println("init s3 bucket success...")
	return sess
}

func OssGetByteObject(bucket *oss.Bucket, fileName string) (*bytes.Buffer, error) {
	// oss流式下载
	buf := new(bytes.Buffer)
	dataStream, err := bucket.GetObject(fileName)
	if err != nil {
		return buf, err
	}
	io.Copy(buf, dataStream)
	dataStream.Close()
	return buf, err
}

func S3UploadObject(bucketName string, session *session.Session, fileStream *bytes.Buffer, fileName string) (string, error) {
	// s3 上传
	buffer := make([]byte, fileStream.Len())
	fileStream.Read(buffer)
	_, err := s3.New(session).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(fileName),
		ACL:                  aws.String("public-read"),
		Body: 				  bytes.NewReader(buffer),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
		StorageClass:         aws.String("STANDARD"),
	})
	if err != nil {
		return "", err
	}
	return fileName, err
}

func main() {
	config := ReadConfig()
	ossBucket := InitOss(config)
	s3Bucket := InitS3(config)
	marker := oss.Marker("")

	for {
		lor, err := ossBucket.ListObjects(oss.MaxKeys(1000), marker)
		if err != nil {
			RaiseError(err)
		}
		marker = oss.Marker(lor.NextMarker)
		for _, obj := range lor.Objects {
			i := 0
			for {
				i ++
				if i>3 {
					fmt.Println("obj key: ", obj.Key, "put 3 count failed")
					break
				}
				_buffer, err := OssGetByteObject(ossBucket, obj.Key)
				if err != nil {
					fmt.Println(err)
					continue
				}
				_, err = S3UploadObject(config.S3.BucketName, s3Bucket, _buffer, obj.Key)
				if err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Println("copy object success, key name: ", obj.Key)
				break
			}
		}
		if !lor.IsTruncated {
			break
		}
	}
	fmt.Println("All files copied ")
	os.Exit(1)
}