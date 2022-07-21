package main

import (
	"fmt"
)

// A user is the object that is stored in the processor's group table
type user struct {
	EarlierTime string
	LatestTime  string
	TotalValue  int32
}

// This codec allows marshalling (encode) and unmarshalling (decode) the user to and from the
// group table.
type userCodec struct{}

func serialiseMap(value interface{}) map[string]interface{} {
	userMap := make(map[string]interface{})

	userMap["EarlierTime"] = value.(*user).EarlierTime
	userMap["LatestTime"] = value.(*user).LatestTime
	userMap["TotalValue"] = value.(*user).TotalValue

	return userMap
}

func deSerialiseMap(value interface{}) user {
	var newUser user

	newUser.EarlierTime = value.(map[string]interface{})["EarlierTime"].(string)
	newUser.LatestTime = value.(map[string]interface{})["LatestTime"].(string)
	newUser.TotalValue = value.(map[string]interface{})["TotalValue"].(int32)

	return newUser
}

// Encodes a user into []byte
func (codec userCodec) Encode(value interface{}) ([]byte, error) {
	if _, isUser := value.(*user); !isUser {
		return nil, fmt.Errorf("Codec requires value *user, got %T", value)
	}

	userMap := serialiseMap(value)

	binary, err := avrocodec.BinaryFromNative(nil, userMap)
	if err != nil {
		return nil, fmt.Errorf("Error marshaling user: %v", err)
	}
	return binary, nil
}

// Decodes a user from []byte to it's go representation.
func (codec userCodec) Decode(data []byte) (interface{}, error) {
	native, _, err := avrocodec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling user: %v", err)
	}

	newUser := deSerialiseMap(native)
	return &newUser, nil
}
