package main

import (
	"github.com/99-66/go-kafka-protobuf-producer-example/kafka"
	pb "github.com/99-66/go-kafka-protobuf-producer-example/protos/addressbook"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
)

// ChannelData Sample 데이터를 전달하기 위한 채널용 모델을 선언한다
type ChannelData struct {
	Bytes []byte
	Err   error
}

func main() {
	// Initializing Kafka Producer
	p, err := kafka.Init()
	if err != nil {
		panic(err)
	}
	defer p.Close()

	topic := kafka.Topic()

	ch := make(chan ChannelData)
	// sample 데이터를 생성한다
	go generateSample(ch)

	// 채널에서 sample 데이터를 읽어온다
	for v := range ch {
		if v.Err != nil {
			log.Printf("%s\n", err)
			continue
		}
		// producer 로 데이터를 전달한다
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(v.Bytes),
		}
		p.Input() <- msg
		log.Printf("sent kafka...")
	}
}

func generateSample(ch chan<- ChannelData) {
	for i := 0; i < 10; i++ {
		// Sample protobuf 데이터를 생성한다
		p := pb.Person{
			Id:    1234,
			Name:  "John Doe",
			Email: "jdoe@example.com",
			Phones: []*pb.Person_PhoneNumber{
				{Number: "010-1234-4567", Type: pb.Person_MOBILE},
			},
			LastUpdated: timestamppb.Now(),
		}
		// Protobuf 데이터를 마샬링한다
		encodedPerson, err := proto.Marshal(&p)

		// 에러가 발생하는 경우에는 채널로 에러를 전달한다
		if err != nil {
			ch <- ChannelData{
				Bytes: nil,
				Err:   err,
			}
			log.Printf("generated failed...%d\n", i)
			continue
		}

		// 에러가 발생하지 않는 경우에는 마샬링한 protobuf 데이터를 전달한다
		ch <- ChannelData{
			Bytes: encodedPerson,
			Err:   nil,
		}

		log.Printf("generated sample...%d\n", i)
	}

	defer close(ch)
}
