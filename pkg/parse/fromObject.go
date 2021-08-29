package parse

import "encoding/json"

func ParseObject(data interface{}) (*Entity, error) {

	serialised, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	entity := &Entity{}
	if err := json.Unmarshal(serialised, entity); err != nil {
		return nil, err
	}

	return entity, nil
}
