package server

import (
	"charlie/model"
	"charlie/utils"
	"sync"
)

type Device struct {
	Id         int64 // corresponds to sqlite id field in table
	UUID       string
	DeviceType string
	AuthToken  *AuthToken

	downstreamConnection    chan struct{}
	downstreamChangesBuffer *utils.SafeSlice[model.Change]
	fileRequestChan         chan FileRequest
	authMu                  sync.Mutex
}

func NewDevice(id int64, uuid, deviceType string, authToken *AuthToken, bufferCap int) (*Device, error) {
	device := &Device{
		Id:                      id,
		UUID:                    uuid,
		DeviceType:              deviceType,
		AuthToken:               authToken,
		downstreamConnection:    make(chan struct{}, 1),
		downstreamChangesBuffer: utils.NewSafeSlice[model.Change](bufferCap),
		fileRequestChan:         make(chan FileRequest),
	}

	return device, nil
}
