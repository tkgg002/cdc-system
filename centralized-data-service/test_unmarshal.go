package main

import (
"encoding/json"
"fmt"
)

type CDCEventData struct {
	Op     string                 `json:"op"`
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
}
type CDCEvent struct {
	Data CDCEventData `json:"data"`
}

func main() {
	payload := `{"data": {"op": "u", "after": "{\"ok\": 1}"}}`
	var event CDCEvent
	err := json.Unmarshal([]byte(payload), &event)
	fmt.Printf("String after error: %v\n", err)

	payload2 := `{"data": {"op": "u", "after": null, "patch": "{\"\\$set\": {\"ok\": 1}}" }}`
	var event2 CDCEvent
	err = json.Unmarshal([]byte(payload2), &event2)
	fmt.Printf("Null after error: %v, after=%v\n", err, event2.Data.After)
}
