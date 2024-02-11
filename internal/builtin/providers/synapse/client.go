package synapse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/go-retryablehttp"
)

type ExampleClient struct {
	HttpClient       *retryablehttp.Client
	Endpoint         string
	WorkingDirectory string
	OutputDirectory  string
	BuildDirectory   string
}

type ExampleError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Stack   string `json:"stack"`
}

func (e ExampleError) Error() string {
	return fmt.Sprintf("Received error from service: %s (%s)\n%s", e.Message, e.Code, e.Stack)
}

func (client ExampleClient) sendRequest(path string, data []byte) ([]byte, error) {
	body := bytes.NewBuffer(data)
	url := client.Endpoint + path

	resp, err := client.HttpClient.Post(url, "application/json", body)
	if err != nil {
		return nil, err
	}

	var contentLength int64
	if resp.ContentLength == -1 {
		contentLength = 10000000 // FIXME
	} else {
		contentLength = resp.ContentLength
	}

	buffer := make([]byte, contentLength)
	_, err = io.ReadAtLeast(resp.Body, buffer, int(contentLength))
	if err != nil {
		return nil, err
	}

	result := buffer[:contentLength]
	if resp.StatusCode != 200 {
		var customErr ExampleError
		err = json.Unmarshal(result, &customErr)
		if err != nil {
			return nil, err
		}
		return nil, customErr
	}

	resp.Body.Close()

	return result, nil
}
