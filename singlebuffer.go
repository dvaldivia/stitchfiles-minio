package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio-go"
	"io"
	"log"
	"os"
	"time"
)

func main() {
	fmt.Println("hello")
	log.Println("lets stich")

	// Get minio client
	storage_host := "localhost"
	if os.Getenv("T_STORAGE_HOST") != "" {
		storage_host = os.Getenv("T_STORAGE_HOST")
	}
	storagePort := "9000"
	if os.Getenv("T_STORAGE_PORT") != "" {
		storagePort = os.Getenv("T_STORAGE_PORT")
	}

	storage_bucket := "myapp"
	if os.Getenv("T_STORAGE_BUCKET") != "" {
		storage_bucket = os.Getenv("T_STORAGE_BUCKET")
	}
	var endpoint string
	if storagePort == "x" {
		endpoint = storage_host
	} else {
		endpoint = storage_host + ":" + storagePort
	}
	log.Println("endpoint", endpoint)
	accessKeyID := "myapp"
	if os.Getenv("ACCESS_KEY") != "" {
		accessKeyID = os.Getenv("ACCESS_KEY")
	}
	secretAccessKey := "storagemyapp"
	if os.Getenv("SECRET_KEY") != "" {
		secretAccessKey = os.Getenv("SECRET_KEY")
	}
	useSSL := false
	if os.Getenv("T_STORAGE_SSL") != "" {
		if os.Getenv("T_STORAGE_SSL") == "true" {
			useSSL = true
		}
	}

	uploadFileToMinio := func(minioClient *minio.Client, bucket string, reader io.Reader, fileName string) chan interface{} {
		result := make(chan interface{})
		go func() {
			defer close(result)
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			n, err := minioClient.PutObjectWithContext(ctx,
				storage_bucket,
				fileName,
				reader,
				-1,
				minio.PutObjectOptions{
					ContentType: "application/octet-stream",
				})
			if err != nil {
				log.Print("UPLOADS0005")
				log.Println(err)
				return
			}
			fmt.Println("Successfully uploaded bytes: ", n)
		}()
		return result
	}

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	parts := []string{
		"test/part-0",
		"test/part-1",
		"test/part-2",
		"test/part-3",
		"test/part-4",
		"test/part-5",
		"test/part-6",
	}

	fileName := "test/image.jpg"

	megaBuffer := bytes.Buffer{}

	uploadReader, uploadWriter := io.Pipe()

	buffer := make([]byte, 64*1024)

	uploadToMinioCh := uploadFileToMinio(minioClient, storage_bucket, uploadReader, fileName)
	for _, part := range parts {

		object, err := minioClient.GetObject(storage_bucket, part, minio.GetObjectOptions{})
		if err != nil {
			fmt.Println(err)
			return
		}
		log.Println("done reading, now stream it")

		for {

			_, err := object.Read(buffer)

			// If we didn't got EOF write to writers and stream back
			if err != io.EOF {
				//uploadWriter.Write(buffer)
				megaBuffer.Write(buffer)
			}

			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Println(err)
				}
			}

		}

		log.Println("done with part", part)
	}
	megaBuffer.WriteTo(uploadWriter)
	uploadWriter.Close()

	<-uploadToMinioCh
}
