package model

import (
	"encoding/json"
	"errors"
)

type Entry struct {
	UUID       string `json:"id"`
	DeviceUUID string `json:"device_id"` // references the uuid of the device
	CreatedAt  int64  `json:"created_at"`

	Body map[string]interface{} `json:"body"`
}

type Change struct {
	Id        int64  `json:"-"` // references the id of the persisted change
	Operation string `json:"operation"`
	Entry     *Entry `json:"entry,omitempty"`
	EntryUUID string `json:"entry_id,omitempty"`
}

type Commit struct {
	Changes []Change `json:"changes"`
}

func (c *Change) UnmarshalJSON(data []byte) error {
	var aux struct {
		Operation string `json:"operation"`
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	c.Operation = aux.Operation

	switch aux.Operation {
	case "ADD":
		var addStruct struct {
			Operation string `json:"operation"`
			Entry     Entry  `json:"entry"`
		}
		if err := json.Unmarshal(data, &addStruct); err != nil {
			return err
		}
		c.Entry = &addStruct.Entry

	case "REMOVE":
		var removeStruct struct {
			Operation string `json:"operation"`
			EntryUUID string `json:"entry_id"`
		}
		if err := json.Unmarshal(data, &removeStruct); err != nil {
			return err
		}
		c.EntryUUID = removeStruct.EntryUUID

	default:
		return errors.New("unknown operation type")
	}

	return nil
}
