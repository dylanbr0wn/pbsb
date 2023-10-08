package main

import (
	"log"
	"os"

	"github.com/dylanbr0wn/pbsb/broker"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "broker",
		Usage: "subscribe to channel",
		Action: func(cCtx *cli.Context) error {
			port := cCtx.Args().Get(0)
			if port == "" {
				log.Fatal("broker address required")
			}
			broker := broker.NewBroker()
			broker.Serve(port)
			return nil
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
