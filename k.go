package main

import (
	"bufio"
	"bytes"
	"log"

	"github.com/controlgroup/gaws/kinesis"

	"fmt"
	"os"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <Kinesis Stream>\n", os.Args[0])
		os.Exit(1)
	}

	ks := kinesis.KinesisService{Endpoint: "https://kinesis.us-east-1.amazonaws.com"}
	reader := bufio.NewReader(os.Stdin)
	stream := &kinesis.Stream{Name: os.Args[1], Service: &ks}
	var b bytes.Buffer
	bytesSent := 0
	recordsSent := 0

	for {

		line, err := reader.ReadBytes('\n')
		if err != nil {
			// You may check here if err == io.EOF
			break
		}

		if b.Len()+len(line) > 50000 {
			err := stream.PutRecord("foo", b.Bytes())

			if err != nil {
				log.Fatal(err)
			}

			bytesSent += b.Len()
			recordsSent += 1
			b.Reset()

		}
		b.Write(line)

	}
	if b.Len() > 0 {
		err := stream.PutRecord("foo", b.Bytes())
		if err != nil {
			log.Fatal(err)
		}
		bytesSent += b.Len()
		recordsSent += 1
	}
	fmt.Printf("Sent %d bytes in %d records to Kinesis\n", bytesSent, recordsSent)
}
