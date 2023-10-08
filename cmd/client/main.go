package main

import (
	"log"
	"os"

	"github.com/dylanbr0wn/pbsb/client" // imports as package "cli"
	cli "github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "client",
		Usage: "subscribe to channel",
		Commands: []*cli.Command{
			{
				Name:    "subscribe",
				Aliases: []string{"s"},
				Usage:   "subscribe to channel",
				Action: func(cCtx *cli.Context) error {
					client := client.NewClient()
					addr := cCtx.Args().Get(0)
					if addr == "" {
						log.Fatal("broker address required")
					}
					channel := cCtx.Args().Get(1)
					if channel == "" {
						log.Fatal("channel name required")
					}
					if err := client.Connect(addr); err != nil {
						log.Fatal(err)
					}
					if err := client.Subscribe(channel); err != nil {
						log.Fatal(err)
					}
					if err := client.Listen(); err != nil {
						log.Fatal(err)
					}
					return nil
				},
			},
			{
				Name:    "publish",
				Aliases: []string{"p"},
				Usage:   "publish message to channel",
				Action: func(cCtx *cli.Context) error {
					client := client.NewClient()
					addr := cCtx.Args().Get(0)
					if addr == "" {
						log.Fatal("broker address required")
					}
					channel := cCtx.Args().Get(1)
					if channel == "" {
						log.Fatal("channel name required")
					}
					msg := cCtx.Args().Get(2)
					if msg == "" {
						log.Fatal("nonempy message required")
					}

					if err := client.Connect(addr); err != nil {
						log.Fatal(err)
					}

					if err := client.Publish(channel, msg); err != nil {
						log.Fatal(err)
					}
					return nil
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}
