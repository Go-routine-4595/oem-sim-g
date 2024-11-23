package service

import (
	"github.com/Go-routine-4595/oem-sim-g/model"
	"math/rand"
	"sync"
	"time"
)

type ISendAlarm interface {
	SendAlarm(events model.Events) error
}

type Service struct {
	gateway ISendAlarm
	lock    sync.Mutex
}

func NewService(g ISendAlarm) *Service {
	return &Service{
		gateway: g,
	}
}

func (s *Service) CreateAlarm(assetName string, oemAlarm string) {
	var (
		event    model.AssetEvent
		aDataLat model.AssociatedData
		aDataLng model.AssociatedData
		events   model.Events
		r        int
	)

	r = rand.Intn(100)
	//s.lock.Lock()

	aDataLat = model.AssociatedData{
		Name:      "GPSLatitudeofEquipment",
		Quality:   "High",
		Timestamp: time.Now(),
		Value:     getFakeLat(),
	}

	aDataLng = model.AssociatedData{
		Name:      "GPSLongitudeofEquipment",
		Quality:   "High",
		Timestamp: time.Now(),
		Value:     getFakeLgt(),
	}

	event = model.AssetEvent{
		AssetName:      assetName,
		AssociatedData: []model.AssociatedData{aDataLat, aDataLng},
		EventName:      oemAlarm,
		EventStatus:    "InActive",
		Timestamp:      time.Now(),
		CreatedUser:    "System",
	}

	events.AssetEvents = append(events.AssetEvents, event)
	s.gateway.SendAlarm(events)

	if r%2 == 0 {
		time.Sleep(time.Millisecond * time.Duration(10))
		event = model.AssetEvent{
			AssetName:      assetName,
			AssociatedData: []model.AssociatedData{aDataLat, aDataLng},
			EventName:      oemAlarm,
			EventStatus:    "Active",
			Timestamp:      time.Now(),
			CreatedUser:    "System",
		}
		events.AssetEvents = append(events.AssetEvents, event)
		s.gateway.SendAlarm(events)

	}

}

func getFakeLat() float64 {
	return 34.58 + float64(rand.Intn(120))/5000.0
}

func getFakeLgt() float64 {
	return -113.23 + float64(rand.Intn(120))/5000.0
}
