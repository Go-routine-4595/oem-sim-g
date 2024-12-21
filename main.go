package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Go-routine-4595/oem-sim-g/adapters/controller"
	"github.com/Go-routine-4595/oem-sim-g/adapters/gateway/display"
	"github.com/Go-routine-4595/oem-sim-g/adapters/gateway/event-hub"
	"github.com/Go-routine-4595/oem-sim-g/adapters/gateway/mqtt"
	"github.com/Go-routine-4595/oem-sim-g/adapters/gateway/rabbitmq"
	"github.com/Go-routine-4595/oem-sim-g/model"
	"github.com/Go-routine-4595/oem-sim-g/service"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

type Config struct {
	event_hub.EventHubConfig    `yaml:"EventHubConfig"`
	controller.ControllerConfig `yaml:"ControllerConfig"`
	rabbitmq.RabbitMQConfig     `yaml:"RabbitConfig"`
	mqtt.MqttConf               `yaml:"Mqtt"`
}

const (
	Display = iota
	EventHub
	Rabbit
	Mqtt
)

func main() {
	var configFile string
	var rootCmd = &cobra.Command{
		Use:   "oem-sim-g",
		Short: "A simple CLI app to generate OEM alarms, it requires a config file (default config.yaml)",
		Long:  "A simple CLI app to generate OEM alarms, it can output on the standard stdout or sends the event ot a RabbitMQ or an Event Hub broker it requires a config file (default config.yaml)",
	}

	// Define the global flag for the configuration file
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "config.yaml", "Path to the configuration file")

	var displayCmd = &cobra.Command{
		Use:   "display",
		Short: "Display the event on stdout",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Displaying information...")
			// Add your display logic here
			execute(configFile, Display)
		},
	}

	var sendCmd = &cobra.Command{
		Use:   "send",
		Short: "Send OEM alarms to RabbitMQ or Event Hub",
		Run: func(cmd *cobra.Command, args []string) {
			eh, _ := cmd.Flags().GetBool("eh")
			rmq, _ := cmd.Flags().GetBool("rmq")
			cmqtt, _ := cmd.Flags().GetBool("mqtt")

			if !eh && !rmq && !cmqtt {
				fmt.Println("Error: You must provide at least one of the flags --eh or --rmq")
				cmd.Help() // Display help information
				os.Exit(1)
			}

			if eh {
				fmt.Println("Sending via Event Hub")
				execute(configFile, EventHub)
			}
			if rmq {
				fmt.Println("Sending via RabbitMQ")
				execute(configFile, Rabbit)
			}
			if cmqtt {
				fmt.Println("Sending via MQTT")
				execute(configFile, Mqtt)
			}
		},
	}

	// Define flags for send command
	sendCmd.Flags().Bool("eh", false, "Send using Event Hub (end-point and Event Hub defined in config.yaml)")
	sendCmd.Flags().Bool("rmq", false, "Send using RabbitMQ (end-point and RabbitMQ defined in config.yaml)")
	sendCmd.Flags().Bool("mqtt", false, "Send using MQTT (end-point and MQTT defined in config.yaml)")

	rootCmd.AddCommand(displayCmd)
	rootCmd.AddCommand(sendCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}

func execute(file string, sysConf int) {
	var (
		conf   Config
		svr    controller.Controller
		svc    model.IService
		gtw    display.Display
		eh     *event_hub.EventHub
		rabbit *rabbitmq.RabbitMQ
		cmqtt  *mqtt.Mqtt
		ctx    context.Context
		cancel context.CancelFunc
		sig    chan os.Signal
		wg     *sync.WaitGroup
		wgG    *sync.WaitGroup
		err    error
	)

	wg = &sync.WaitGroup{}
	wgG = &sync.WaitGroup{}
	ctx, cancel = context.WithCancel(context.Background())

	conf = openConfigFile(file)

	//
	switch sysConf {
	case Display:
		gtw = display.NewDisplay()
		svc = service.NewService(gtw)
	case EventHub:
		eh, err = event_hub.NewEventHub(ctx, wg, conf.EventHubConfig)
		if err != nil {
			log.Fatal().Err(err)
		}
		svc = service.NewService(eh)
	case Rabbit:
		conf.RabbitMQConfig.ConnectionString, err = connectionString(
			conf.RabbitMQConfig.Scheme,
			conf.RabbitMQConfig.Host,
			conf.RabbitMQConfig.UserName,
			conf.RabbitMQConfig.Password,
			conf.RabbitMQConfig.Port,
			conf.RabbitMQConfig.Resource)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to create connection string")
		}
		rabbit = rabbitmq.NewRabbitMQ(conf.RabbitMQConfig)
		// Start the rabbit
		rabbit.Start(ctx, wg)
		svc = service.NewService(rabbit)
	case Mqtt:
		cmqtt, err = mqtt.NewMqtt(conf.MqttConf, 0, ctx, wg)
		//
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to create connection string")
		}
		svc = service.NewService(cmqtt)
	}

	svr = controller.NewController(conf.ControllerConfig, svc)
	//svr.Test()

	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()
	// start the Event generator
	svr.Start(ctx, wgG)
	// wait until it finishes
	wgG.Wait()

	fmt.Println("Shutting down...")
	// then we send the cancel to all gateway we are done now
	cancel()
	// wait gateway to teardown there connection cleanly
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

func connectionString(scheme string, base string, user string, pass string, port int, resource string) (string, error) {
	var (
		encodedPass string
		encodedUser string
		encodedRes  string
	)

	if port == 0 {
		return "", errors.New("port is required")
	}
	if scheme == "" {
		return "", errors.New("scheme is required")
	}
	if base == "" {
		return "", errors.New("baseurl is required")
	}
	if scheme != "amqp" && scheme != "amqps" && scheme != "mqtt" && scheme != "mqtts" {
		return "", errors.New("scheme must be amqp, amqps, mqtt or mqtts")
	}

	encodedPass = url.QueryEscape(pass)
	encodedUser = url.QueryEscape(user)
	encodedRes = url.QueryEscape(resource)
	if user == "" && pass == "" {
		return fmt.Sprintf("%s://%s:%d/%s", scheme, base, port, encodedRes), nil
	}
	if pass == "" {
		return fmt.Sprintf("%s://%s@%s:%d/%s", scheme, encodedUser, base, port, encodedRes), nil
	}
	if user == "" {
		return fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, encodedUser, encodedPass, base, port, encodedRes), nil
	}
	return fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, encodedUser, encodedPass, base, port, encodedRes), nil

}
