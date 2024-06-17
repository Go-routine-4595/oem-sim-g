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
	"github.com/Go-routine-4595/oem-sim-g/adapters/gateway/display"
	"github.com/Go-routine-4595/oem-sim-g/adapters/gateway/event-hub"
	"github.com/Go-routine-4595/oem-sim-g/service"

	"gopkg.in/yaml.v3"
)

type Config struct {
	event_hub.EventHubConfig    `yaml:"EventHubConfig"`
	controller.ControllerConfig `yaml:"ControllerConfig"`
}

func main() {
	var (
		conf   Config
		svr    controller.Controller
		svc    model.IService
		gtw    display.Display
		ctx    context.Context
		cancel context.CancelFunc
		sig    chan os.Signal
		wg     *sync.WaitGroup
		args   []string
	)

	args = os.Args

	wg = &sync.WaitGroup{}

	if len(args) == 1 {
		conf = openConfigFile("")
	} else {
		conf = openConfigFile(args[1])
	}

	gtw = display.NewDisplay()
	svc = service.NewService(gtw)

	svr = controller.NewController(conf.ControllerConfig, svc)
	svr.Test()
	ctx, cancel = context.WithCancel(context.Background())
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
