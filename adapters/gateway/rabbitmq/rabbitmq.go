package rabbitmq

import (
	"context"
	"encoding/json"
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
}

type RabbitMQ struct {
	ConnectionString string
	QueueName        string
	msgs             chan []byte
	logger           zerolog.Logger
	conn             *amqp.Connection
	ch               *amqp.Channel
}

func NewRabbitMQ(config RabbitMQConfig) *RabbitMQ {
	return &RabbitMQ{
		msgs:             make(chan []byte),
		ConnectionString: config.ConnectionString,
		QueueName:        config.QueueName,
		logger:           zerolog.New(os.Stdout).Level(zerolog.Level(zerolog.DebugLevel)).With().Timestamp().Logger(),
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
	r.msgs <- msg
	return nil
}

// connect establishes a new connection and channel
func (r *RabbitMQ) connect() error {
	var (
		err error
	)
	r.conn, err = amqp.Dial(r.ConnectionString)
	if err != nil {
		return err
	}

	r.ch, err = r.conn.Channel()
	if err != nil {
		return err
	}

	_, err = r.ch.QueueDeclare(
		r.QueueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return err
	}

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

// close gracefully shuts down the connection and channel
func (r *RabbitMQ) Close() error {
	var err error

	err = r.conn.Close()
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQ) consume(ctx context.Context, wg *sync.WaitGroup) {
	var (
		err error
	)
	wg.Add(1)

	go func() {
		for {
			for msg := range r.msgs {
				// Publish a message
				err = r.ch.Publish(
					"",          // Exchange
					r.QueueName, // Routing key (queue name)
					false,       // Mandatory
					false,       // Immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(msg),
					},
				)
				if err != nil {
					r.logger.Error().Err(err).Msg("Failed to publish a message")
					r.reconnect()
				}
			}
		}
	}()

	r.logger.Info().Msg("Wating")
	<-ctx.Done()
	// disconnect gracefully and leave
	r.Close()
	r.logger.Info().Msg("Received interrupt signal, closing connection")
	wg.Done()
	return
}
