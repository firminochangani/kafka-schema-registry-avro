package consumer

import (
	"context"

	"github.com/urfave/cli/v3"
)

func New() *cli.Command {
	return &cli.Command{
		Name:   "consumer",
		Action: run,
	}
}

func run(ctx context.Context, command *cli.Command) error {
	return nil
}
