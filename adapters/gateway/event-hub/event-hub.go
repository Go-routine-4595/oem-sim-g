package event_hub

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Go-routine-4595/oem-sim-g/model"
	"log"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
)

// connection string can have the event hub name like this
// Endpoint=sb://FctsNAMemNADevlEvtHub01.servicebus.windows.net/;SharedAccessKeyName=<KeyName>;SharedAccessKey=<KeyValue>;EntityPath=honeywell-uas-oem-alarms
// see https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-get-connection-string
// see https://azure.github.io/azure-sdk/golang_introduction.html
// see https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs#ConsumerClient.Close
// see https://github.com/Azure/azure-sdk-for-go?tab=readme-ov-file

type EventHubConfig struct {
	Connection   string `yaml:"connection"`
	EventHubName string `yaml:"EventHubName"`
}

type EventHub struct {
	producerClient *azeventhubs.ProducerClient
}

func NewEventHub(ctx context.Context, wg *sync.WaitGroup, conf EventHubConfig) (*EventHub, error) {
	var (
		err            error
		producerClient *azeventhubs.ProducerClient
	)
	producerClient, err = azeventhubs.NewProducerClientFromConnectionString(conf.Connection, conf.EventHubName, nil)

	if err != nil {
		return nil, errors.Join(err, errors.New("failed to create producer client"))
	}

	go func() {
		<-ctx.Done()
		err = producerClient.Close(ctx)
		if err != nil {
			log.Printf("failed to close producer client: %s", err)
		}
		wg.Done()
	}()

	return &EventHub{
		producerClient: producerClient,
	}, nil
}

func (e EventHub) SendAlarm(events model.Events) error {
	var (
		eventData       []byte
		err             error
		msg             *azeventhubs.EventData
		newBatchOptions *azeventhubs.EventDataBatchOptions
	)

	eventData, err = json.Marshal(events)
	if err != nil {
		return errors.Join(err, errors.New("failed to marshal event display.CreateAlarm"))
	}

	newBatchOptions = &azeventhubs.EventDataBatchOptions{
		// The options allow you to control the size of the batch, as well as the partition it will get sent to.

		// PartitionID can be used to target a specific partition ID.
		// specific partition ID.
		//
		// PartitionID: partitionID,

		// PartitionKey can be used to ensure that messages that have the same key
		// will go to the same partition without requiring your application to specify
		// that partition ID.
		//
		// PartitionKey: partitionKey,

		//
		// Or, if you leave both PartitionID and PartitionKey nil, the service will choose a partition.
	}

	// Creates an EventDataBatch, which you can use to pack multiple events together, allowing for efficient transfer.
	batch, err := e.producerClient.NewEventDataBatch(context.TODO(), newBatchOptions)
	if err != nil {
		return errors.Join(err, errors.New("failed to create event data batch"))
	}

	msg = createEventForAlarm(eventData)
	if err = e.addEventToBatch(batch, msg, newBatchOptions); err != nil {
		return err
	}

	return e.sendRemainingBatch(batch)

}

// addEventToBatch handles adding an event to a batch, retrying if necessary when the batch is full.
func (e EventHub) addEventToBatch(batch *azeventhubs.EventDataBatch, event *azeventhubs.EventData, options *azeventhubs.EventDataBatchOptions) error {
	for {
		if err := batch.AddEventData(event, nil); err != nil {
			if errors.Is(err, azeventhubs.ErrEventDataTooLarge) {
				if batch.NumEvents() == 0 {
					return errors.Join(err, errors.New("single event is too large for a batch"))
				}
				// This batch is full - we can send it and create a new one and continue
				// packaging and sending events.
				if err := e.producerClient.SendEventDataBatch(context.TODO(), batch, nil); err != nil {
					return errors.Join(err, errors.New("failed to send current batch"))
				}
				// create the next batch we'll use for events, ensuring that we use the same options
				// each time so all the messages go the same target.
				newBatch, newBatchErr := e.producerClient.NewEventDataBatch(context.TODO(), options)
				if newBatchErr != nil {
					return errors.Join(newBatchErr, errors.New("failed to create a new batch after sending"))
				}
				batch = newBatch
				continue
			}
			return errors.Join(err, errors.New("unexpected error while adding event to batch"))
		}
		break
	}
	return nil
}

// sendRemainingBatch sends any remaining events in a batch.
func (e EventHub) sendRemainingBatch(batch *azeventhubs.EventDataBatch) error {
	if batch.NumEvents() > 0 {
		if err := e.producerClient.SendEventDataBatch(context.TODO(), batch, nil); err != nil {
			return errors.Join(err, errors.New("failed to send remaining batch"))
		}
	}
	return nil
}

func createEventForAlarm(buf []byte) *azeventhubs.EventData {
	return &azeventhubs.EventData{
		Body: buf,
	}
}
