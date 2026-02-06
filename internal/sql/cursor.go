package sql

import (
	"encoding/base64"
	"encoding/json"
)

// EncodeCursor encodes a ResourcesCursor to a base64 string.
func EncodeCursor(c *ResourcesCursor) EncodedCursor {
	if c == nil {
		return ""
	}
	data, _ := json.Marshal(c)
	return EncodedCursor(base64.StdEncoding.EncodeToString(data))
}

// DecodeCursor decodes a base64 string into ResourcesCursor.
func DecodeCursor(s EncodedCursor) (*ResourcesCursor, error) {
	if s == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(string(s))
	if err != nil {
		return nil, err
	}
	var c ResourcesCursor
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
