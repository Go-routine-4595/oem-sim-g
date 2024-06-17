package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Go-routine-4595/oem-sim-g/model"
)

type ControllerConfig struct {
	Frequency      int    `yaml:"Frequency"`
	MaxDataPoint   int    `yaml:"MaxDataPoint"`
	DataDefinition string `yaml:"DataDefinition"`
}
type Controller struct {
	frequency      int
	maxDataPoint   int
	dataDefinition []DataDefinition
	alarmSvc       model.IService
}

type DataDefinition struct {
	AssetID        string   `json:"asset_id"`
	AssetOemAlarms []string `json:"asset_oem_alarm"`
}

func NewController(conf ControllerConfig, a model.IService) Controller {

	f, err := os.Open(conf.DataDefinition)
	if err != nil {
		processError(errors.Join(err, errors.New("open assets.jsonl file")))
	}
	defer f.Close()

	// Create a new Scanner for the file
	// jsonl (json object on each line)
	scanner := bufio.NewScanner(f)

	var dataDef []DataDefinition

	for scanner.Scan() {
		var item DataDefinition

		if len(scanner.Bytes()) != 0 {
			err = json.Unmarshal(scanner.Bytes(), &item)
			if err != nil {
				processError(err)
			}
			dataDef = append(dataDef, item)
		}
	}
	if scanner.Err() != nil {
		fmt.Println(scanner.Err())
	}

	return Controller{
		frequency:      conf.Frequency,
		maxDataPoint:   conf.MaxDataPoint,
		dataDefinition: dataDef,
		alarmSvc:       a,
	}
}

func (c Controller) Start(ctx context.Context, wg *sync.WaitGroup) {

	for _, def := range c.dataDefinition {
		wg.Add(1)
		go func(def DataDefinition) {
			for i := 0; i < c.maxDataPoint; i++ {
				c.alarmSvc.CreateAlarm(def.AssetID, def.AssetOemAlarms[i%len(def.AssetOemAlarms)])
				select {
				case <-ctx.Done():
					fmt.Println("Controller: ", def.AssetID, "context received signal, shutting down...")
					wg.Done()
					return
				default:
					time.Sleep(time.Duration(c.frequency) * time.Second)
				}
			}
			fmt.Println("Controller: ", def.AssetID, " context done")
			wg.Done()
		}(def)
	}
}

func (c Controller) Test() {
	fmt.Println("Controller Test data:")
	fmt.Println(c)
}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}
