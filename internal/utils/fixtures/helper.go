package fixtures

import (
	"encoding/json"
	"strings"
)

func ReadFile(pathToFile string) ([]byte, error) {
	return FixturesFS.ReadFile(pathToFile)
}

func MustReadTextFile(pathToFile string) string {
	content, err := ReadFile(pathToFile)
	if err != nil {
		panic(err)
	}
	return string(content)
}

func MustReadFile(pathToFile string) []byte {
	data, err := ReadFile(pathToFile)
	if err != nil {
		panic(err)
	}

	return data
}

func ReadString(pathToFile string) (string, error) {
	data, err := ReadFile(pathToFile)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(data)), nil
}

func MustReadString(pathToFile string) string {
	data, err := ReadString(pathToFile)
	if err != nil {
		panic(err)
	}

	return data
}

// This is to read and format json data by unmarshalling and marshalling
func ReadJson(pathToFile string) ([]byte, error) {
	data, err := ReadFile(pathToFile)
	if err != nil {
		return nil, err
	}
	var inputUnmarshal interface{}
	err = json.Unmarshal(data, &inputUnmarshal)
	if err != nil {
		return nil, err
	}
	inputMarshal, err := json.Marshal(inputUnmarshal)
	if err != nil {
		return nil, err
	}

	return inputMarshal, nil
}

func MustReadJson(pathToFile string) []byte {
	data, err := ReadJson(pathToFile)
	if err != nil {
		panic(err)
	}

	return data
}
