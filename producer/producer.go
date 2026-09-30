package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/urfave/cli/v3"

	"github.com/firminochangani/kafka-schema-registry-avro/events"
)

func New() *cli.Command {
	return &cli.Command{
		Name:   "producer",
		Action: run,
	}
}

func run(ctx context.Context, command *cli.Command) error {
	kafkaClient, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(os.Getenv("KAFKA_BROKER_URLS"), ",")...))
	if err != nil {
		return err
	}
	defer kafkaClient.Close()
	if err = kafkaClient.Ping(ctx); err != nil {
		return err
	}

	// Confluent schema registry
	schemaRegistryClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(os.Getenv("CONFLUENT_SCHEMA_REGISTRY")))
	if err != nil {
		return err
	}
	defer func() { _ = schemaRegistryClient.Close() }()

	// Avro serializer
	avroConfig := avro.NewSerializerConfig()
	avroConfig.AutoRegisterSchemas = false
	serializer, err := avro.NewSpecificSerializer(schemaRegistryClient, serde.ValueSerde, avroConfig)
	if err != nil {
		return err
	}
	defer func() { _ = serializer.Close() }()
	serializer.SubjectNameStrategy = serde.RecordNameStrategy(getRecordName)

	// serialize the event
	order := fixtureOrderPlacedEvent()
	event, err := serializer.Serialize(events.TopicOrders, &order)
	if err != nil {
		return fmt.Errorf("error serializing event: %v", err)
	}

	err = kafkaClient.ProduceSync(ctx, &kgo.Record{
		Key:   []byte(order.Id),
		Value: event,
		Topic: events.TopicOrders,
	}).FirstErr()
	if err != nil {
		return fmt.Errorf("error producing event: %v", err)
	}

	log.Println("Event produced with success")

	return nil
}

func fixtureOrderPlacedEvent() events.OrderPlaced {
	maxItems := gofakeit.Number(5, 20)
	items := make([]events.OrderPlacedItem, maxItems)

	for i := range maxItems {
		items[i] = events.OrderPlacedItem{
			Product_id: gofakeit.UUID(),
			Name:       gofakeit.ProductName(),
			Price:      int64(gofakeit.Number(1000, 10000)),
			Quantity:   int64(gofakeit.Number(1, 5)),
		}
	}

	return events.OrderPlaced{
		Id:        gofakeit.UUID(),
		Items:     items,
		Placed_at: time.Now().Unix(),
		// Status:    "",
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
