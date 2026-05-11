package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func CalculateHash(data map[string]interface{}) string {
	dataJSON, _ := json.Marshal(data)
	hash := sha256.Sum256(dataJSON)
	return hex.EncodeToString(hash[:])
}
