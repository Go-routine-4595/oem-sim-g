package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Go-routine-4595/oem-sim-g/model"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

type RabbitMQConfig struct {
	ConnectionString string `yaml:"ConnectionString"`
	QueueName        string `yaml:"QueueName"`
	Scheme           string `yaml:"Scheme"`
	Host             string `yaml:"Host"`
	Port             int    `yaml:"Port"`
	UserName         string `yaml:"UserName"`
	Password         string `yaml:"Password"`
	Resource         string `yaml:"Resource"`
}

type RabbitMQ struct {
	ConnectionString string
	QueueName        string
	messageQueue     chan []byte
	logger           zerolog.Logger
	conn             *amqp.Connection
	ch               *amqp.Channel
}

const (
	defaultLogLevel = zerolog.DebugLevel
)

// Factory method for logger
func createLogger() zerolog.Logger {
	return zerolog.New(os.Stdout).
		Level(defaultLogLevel).
		With().
		Timestamp().
		Logger()
}

// NewRabbitMQ initializes the RabbitMQ struct with a predefined logger
func NewRabbitMQ(config RabbitMQConfig) *RabbitMQ {
	return &RabbitMQ{
		messageQueue:     make(chan []byte),
		ConnectionString: config.ConnectionString,
		QueueName:        config.QueueName,
		logger:           createLogger(),
	}
}

func (r *RabbitMQ) SendAlarm(events model.Events) error {
	var (
		msg []byte
		err error
	)

	msg, err = json.Marshal(events)
	if err != nil {
		return err
	}
	r.logger.Debug().Msgf("Sending alarm: %s \n", string(msg))
	r.messageQueue <- msg
	return nil
}

// connect establishes a new connection and channel
func (r *RabbitMQ) connect() error {
	var (
		err  error
		conn *amqp.Connection
		ch   *amqp.Channel
	)
	conn, err = amqp.Dial(r.ConnectionString)
	if err != nil {
		return err
	}

	ch, err = conn.Channel()
	if err != nil {
		_ = conn.Close() // Ensure connection is closed if the channel fails
		return err
	}

	_, err = ch.QueueDeclare(
		r.QueueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		_ = conn.Close() // Close connection on error
		return err
	}

	r.conn = conn
	r.ch = ch

	return nil
}

// reconnect handles reconnection logic
func (r *RabbitMQ) reconnect() {
	for {
		r.logger.Info().Msg("Attempting to reconnect to RabbitMQ...")
		err := r.connect()
		if err == nil {
			r.logger.Info().Msg("Successfully reconnected to RabbitMQ...")
			break
		}
		r.logger.Error().Err(err).Msg("Reconnect failed")
		time.Sleep(5 * time.Second) // Exponential backoff could be implemented here
	}
}

// Start the controller
func (r *RabbitMQ) Start(ctx context.Context, wg *sync.WaitGroup) {
	var err error

	err = r.connect()
	if err != nil {
		r.logger.Fatal().Err(err).Msg("Failed to connect to RabbitMQ")
	}
	go r.consume(ctx, wg)
}

// Close gracefully shuts down the connection and channel
func (r *RabbitMQ) Close() error {
	var err error

	if r.conn == nil {
		return errors.New("connection is already closed or not initialized")
	}
	err = r.conn.Close()
	if err != nil {
		return err
	}

	return nil
}

// consume continuously publishes messages from the queue
func (r *RabbitMQ) consume(ctx context.Context, wg *sync.WaitGroup) {
	var (
		err error
	)
	wg.Add(1)
	r.logger.Info().Msg("Waiting")

	for {
		select {
		case msg := <-r.messageQueue:
			// Publish a message
			err = r.publishMessage(msg)
			if err != nil {
				r.logger.Error().Err(err).Msg("Failed to publish a message")
				r.reconnect()
			}
		case <-ctx.Done():
			r.logger.Info().Msg("Received interrupt signal, closing connection")
			r.Close()
			wg.Done()
			return
		}
	}
}

// publishMessage simplifies message publishing logic
func (r *RabbitMQ) publishMessage(msg []byte) error {
	return r.ch.Publish(
		"",          // Exchange
		r.QueueName, // Routing key (queue name)
		false,       // Mandatory
		false,       // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg,
		},
	)
}
