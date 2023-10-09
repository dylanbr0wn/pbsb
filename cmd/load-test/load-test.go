package main

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dylanbr0wn/pbsb/client"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "client",
		Usage: "subscribe to channel",
		Action: func(cCtx *cli.Context) error {

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

			agents := cCtx.Args().Get(3)
			if msg == "" {
				log.Fatal("nonempy message required")
			}

			// convert string to int
			numAgents, err := strconv.Atoi(agents)
			if err != nil {
				log.Fatal(err)
			}

			// parse int from string
			amount := cCtx.Args().Get(4)
			if msg == "" {
				log.Fatal("nonempy message required")
			}

			// convert string to int
			num, err := strconv.Atoi(amount)
			if err != nil {
				log.Fatal(err)
			}
			totalStart := time.Now()
			var wg sync.WaitGroup
			for i := 0; i < numAgents; i++ {
				wg.Add(1)
				go func(addr, msg, channel string, num int) {
					defer wg.Done()
					client := client.NewClient()
					if err := client.Connect(addr); err != nil {
						log.Fatal(err)
					}

					for i := 0; i < num; i++ {
						if err := client.Publish(channel, msg); err != nil {
							log.Fatal(err)
						}
					}
				}(addr, msg, channel, num)
			}
			wg.Wait()
			totalElapsed := time.Since(totalStart)
			log.Printf("total took %s", totalElapsed)
			log.Printf("total messages %d", numAgents*num)
			log.Printf("messages per second %f", float64(numAgents*num)/totalElapsed.Seconds())
			return nil
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}
