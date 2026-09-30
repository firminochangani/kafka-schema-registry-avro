package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/firminochangani/kafka-schema-registry-avro/events"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/urfave/cli/v3"
)

func New() *cli.Command {
	return &cli.Command{
		Name:   "consumer",
		Action: run,
	}
}

func run(ctx context.Context, command *cli.Command) error {
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(os.Getenv("KAFKA_BROKER_URLS"), ",")...),
		kgo.ConsumerGroup("work.firmino.orders.consumer"),
		kgo.ConsumeTopics(events.TopicOrders),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}

	// Confluent schema registry
	schemaRegistryClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(os.Getenv("CONFLUENT_SCHEMA_REGISTRY")))
	if err != nil {
		return err
	}
	defer func() { _ = schemaRegistryClient.Close() }()

	// Avro deserializer
	avroConfig := avro.NewDeserializerConfig()
	deserializer, err := avro.NewSpecificDeserializer(schemaRegistryClient, serde.ValueSerde, avroConfig)
	if err != nil {
		return err
	}
	defer func() { _ = deserializer.Close() }()
	deserializer.SubjectNameStrategy = serde.RecordNameStrategy(getRecordName)

	for {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled")
		default:
		}

		fetches := kafkaClient.PollFetches(ctx)
		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			var event events.OrderPlaced
			err = deserializer.DeserializeInto(events.TopicOrders, record.Value, &event)
			if err != nil {
				fmt.Printf("error deserializing event: %v\n", err)
			} else {
				fmt.Printf("New event: %v\n", event)
			}

			record.Ack(kgo.AckAccept)
		}

		err = kafkaClient.CommitUncommittedOffsets(ctx)
		if err != nil {
			return err
		}
	}
}

func getRecordName(info schemaregistry.SchemaInfo) (string, error) {
	var s struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	}
	if err := json.Unmarshal([]byte(info.Schema), &s); err != nil {
		return "", err
	}
	if s.Namespace != "" {
		return s.Namespace + "." + s.Name, nil
	}
	return s.Name, nil
}
