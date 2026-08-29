package notice

import (
	"encoding/json"
	"errors"
)

const Kind = "converge.notification"

type Notification struct {
	ID  string
	All bool
}

type envelope struct {
	ID  string `json:"id,omitempty"`
	All bool   `json:"all,omitempty"`
}

var errUndecodable = errors.New("notice: undecodable notification")

func Encode(id string) ([]byte, error) {
	if id == "" {
		return nil, errors.New("notice: empty id; the whole job is EncodeAll")
	}
	return json.Marshal(envelope{ID: id})
}

func EncodeAll() ([]byte, error) { return json.Marshal(envelope{All: true}) }

func Decode(payload []byte) (Notification, error) {
	var e envelope
	if err := json.Unmarshal(payload, &e); err != nil {
		return Notification{}, errUndecodable
	}
	if e.All {
		if e.ID != "" {
			return Notification{}, errUndecodable
		}
		return Notification{All: true}, nil
	}
	if e.ID == "" {
		return Notification{}, errUndecodable
	}
	return Notification{ID: e.ID}, nil
}
