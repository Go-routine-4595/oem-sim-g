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
		processError(errors.Join(err, fmt.Errorf("file: %s", conf.DataDefinition)))
	}
	defer f.Close()

	// Create a new Scanner for the file
	// jsonl (json object on each line)
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024) // Create a buffer with a larger capacity
	scanner.Buffer(buf, 1024*1024)    // Set the maximum token size to 1 MB

	var dataDef []DataDefinition

	/*
		reader := bufio.NewReader(f)
			for {
				var item DataDefinition

				line, err := reader.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						break
					}
					fmt.Println("Error reading file:", err)
				}
				err = json.Unmarshal(line, &item)
				if err != nil {
					processError(err)
				}
				dataDef = append(dataDef, item)
				fmt.Print(line)
			}
	*/

	for scanner.Scan() {
		var item DataDefinition

		if len(scanner.Bytes()) != 0 {
			//fmt.Println(scanner.Text())
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
			defer wg.Done()
			for i := 0; i < c.maxDataPoint; i++ {
				c.alarmSvc.CreateAlarm(def.AssetID, def.AssetOemAlarms[i%len(def.AssetOemAlarms)])
				select {
				case <-ctx.Done():
					fmt.Println("Controller: ", def.AssetID, "context received signal, shutting down...")
					return
				default:
					time.Sleep(time.Duration(c.frequency) * time.Millisecond)
				}
			}
			fmt.Println("Controller: ", def.AssetID, " done")
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
