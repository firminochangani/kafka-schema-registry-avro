package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v3"

	"github.com/firminochangani/kafka-schema-registry-avro/consumer"
	"github.com/firminochangani/kafka-schema-registry-avro/producer"
)

func main() {
	cmd := &cli.Command{
		Name:        "ecommerce",
		Description: "E-commerce producer and consumer",
		Commands: []*cli.Command{
			producer.New(),
			consumer.New(),
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	err := cmd.Run(ctx, os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
