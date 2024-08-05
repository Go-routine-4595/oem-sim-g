package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/Go-routine-4595/oem-sim-g/model"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Go-routine-4595/oem-sim-g/adapters/controller"
	"github.com/Go-routine-4595/oem-sim-g/adapters/gateway/event-hub"
	"github.com/Go-routine-4595/oem-sim-g/adapters/gateway/rabbitmq"
	"github.com/Go-routine-4595/oem-sim-g/service"

	"gopkg.in/yaml.v3"
)

type Config struct {
	event_hub.EventHubConfig    `yaml:"EventHubConfig"`
	controller.ControllerConfig `yaml:"ControllerConfig"`
	rabbitmq.RabbitMQConfig     `yaml:"RabbitConfig"`
}

func main() {
	var (
		conf Config
		svr  controller.Controller
		svc  model.IService
		//gtw  display.Display
		//eh     *event_hub.EventHub
		rabbit *rabbitmq.RabbitMQ
		ctx    context.Context
		cancel context.CancelFunc
		sig    chan os.Signal
		wg     *sync.WaitGroup
		args   []string
		//err    error
	)

	args = os.Args

	wg = &sync.WaitGroup{}
	ctx, cancel = context.WithCancel(context.Background())

	if len(args) == 1 {
		conf = openConfigFile("config.yaml")
	} else {
		conf = openConfigFile(args[1])
	}

	// an event hub
	/*
		eh, err = event_hub.NewEventHub(ctx, wg, conf.EventHubConfig)
		if err != nil {
			log.Println(err)
			// or a Display if we fail to initiate a new event hub
			gtw = display.NewDisplay()
			svc = service.NewService(gtw)
		} else {
			svc = service.NewService(eh)
		}
	*/

	rabbit = rabbitmq.NewRabbitMQ(conf.RabbitMQConfig)

	// Start the rabbit
	rabbit.Start(ctx, wg)

	svc = service.NewService(rabbit)

	svr = controller.NewController(conf.ControllerConfig, svc)
	svr.Test()

	svr.Start(ctx, wg)

	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()
	wg.Wait()
}

func openConfigFile(s string) Config {
	if s == "" {
		s = "config.yaml"
	}

	f, err := os.Open(s)
	if err != nil {
		processError(errors.Join(err, errors.New("open config.yaml file")))
	}
	defer f.Close()

	var config Config
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&config)
	if err != nil {
		processError(err)
	}
	return config

}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}
