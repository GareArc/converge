package notice

import (
	"encoding/json"
	"errors"
)

const Kind = "converge.notification"

type envelope struct {
	ID string `json:"id"`
}

func Encode(id string) ([]byte, error) { return json.Marshal(envelope{ID: id}) }

func Decode(payload []byte) (string, error) {
	var e envelope
	if err := json.Unmarshal(payload, &e); err != nil {
		return "", errors.New("notice: undecodable notification")
	}
	return e.ID, nil
}
