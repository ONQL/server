package api

import (
	"encoding/json"
	"fmt"
	"onql/storemanager"
)

func handleProtocolRequest(msg *Message) string {
	var command []interface{}
	if err := json.Unmarshal([]byte(msg.Payload), &command); err != nil {
		return errorResponse(fmt.Sprintf("invalid payload: %v", err))
	}

	if len(command) == 0 {
		return errorResponse("empty command")
	}

	cmd, ok := command[0].(string)
	if !ok {
		return errorResponse("invalid command type")
	}

	result, err := executeProtocolCommand(cmd, command[1:])
	if err != nil {
		return errorResponse(err.Error())
	}

	data, _ := json.Marshal(result)
	return string(data)
}

func executeProtocolCommand(cmd string, args []interface{}) (interface{}, error) {
	switch cmd {
	case "desc":
		return descProtocols(args)
	case "set":
		return setProtocol(args)
	case "drop":
		return dropProtocol(args)
	default:
		return nil, fmt.Errorf("unknown command: %s", cmd)
	}
}

func descProtocols(args []interface{}) (interface{}, error) {
	passwords, err := db.GetAllProtocols()
	if err != nil {
		return nil, err
	}

	protocols := make(map[string]interface{})
	for _, password := range passwords {
		proto, err := db.GetProtocol(password)
		if err != nil {
			continue
		}
		protocols[password] = proto
	}

	// Navigate through args if provided
	current := interface{}(protocols)
	for _, key := range args {
		keyStr, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("invalid key type")
		}

		if m, ok := current.(map[string]interface{}); ok {
			if val, exists := m[keyStr]; exists {
				current = val
			} else {
				return nil, fmt.Errorf("protocol not found: %s", keyStr)
			}
		}
	}

	return current, nil
}

func setProtocol(args []interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("set expects 2 args (password, data)")
	}

	password, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid password type")
	}

	dataBytes, _ := json.Marshal(args[1])
	var protocol storemanager.QueryProtocol
	if err := json.Unmarshal(dataBytes, &protocol); err != nil {
		return nil, err
	}

	if err := db.SetProtocol(password, protocol); err != nil {
		return nil, err
	}

	return "success", nil
}

func dropProtocol(args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("drop expects 1 arg (password)")
	}

	password, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid password type")
	}

	if err := db.DeleteProtocol(password); err != nil {
		return nil, err
	}

	return "success", nil
}
