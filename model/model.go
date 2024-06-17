package model

import "time"

type AssociatedData struct {
	Name      string    `json:"Name"`
	Quality   string    `json:"Quality"`
	Timestamp time.Time `json:"Timestamp"`
	Value     float64   `json:"Value"`
}

type AssetEvent struct {
	AssetName      string           `json:"AssetName"`
	AssociatedData []AssociatedData `json:"AssociatedData"`
	EventName      string           `json:"EventName"`
	EventStatus    string           `json:"EventStatus"`
	Timestamp      time.Time        `json:"Timestamp"`
	CreatedUser    string           `json:"CreatedUser"`
}

type Events struct {
	AssetEvents []AssetEvent `json:"Events"`
}

type IService interface {
	CreateAlarm(event string, oemAlarm string)
}
