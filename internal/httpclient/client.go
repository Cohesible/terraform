// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package httpclient

import (
	"fmt"
	"log"
	"net/http"

	cleanhttp "github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/hashicorp/terraform/internal/logging"
)

// New returns the DefaultPooledClient from the cleanhttp
// package that will also send a Terraform User-Agent string.
func New() *http.Client {
	cli := cleanhttp.DefaultPooledClient()
	cli.Transport = &userAgentRoundTripper{
		userAgent: UserAgentString(),
		inner:     cli.Transport,
	}
	return cli
}

func NewRetryableClient() *retryablehttp.Client {
	client := cleanhttp.DefaultPooledClient()
	retryableClient := retryablehttp.NewClient()
	retryableClient.HTTPClient = client
	retryableClient.RetryMax = 2
	retryableClient.RequestLogHook = requestLogHook
	retryableClient.ErrorHandler = maxRetryErrorHandler

	logOutput := logging.LogOutput()
	retryableClient.Logger = log.New(logOutput, "", log.Flags())

	return retryableClient
}

func maxRetryErrorHandler(resp *http.Response, err error, numTries int) (*http.Response, error) {
	// Close the body per library instructions
	if resp != nil {
		resp.Body.Close()
	}

	// Additional error detail: if we have a response, use the status code;
	// if we have an error, use that; otherwise nothing. We will never have
	// both response and error.
	var errMsg string
	if resp != nil {
		errMsg = fmt.Sprintf(": %s returned from %s", resp.Status, hostFromRequest(resp.Request))
	} else if err != nil {
		errMsg = fmt.Sprintf(": %s", err)
	}

	// This function is always called with numTries=RetryMax+1. If we made any
	// retry attempts, include that in the error message.
	if numTries > 1 {
		return resp, fmt.Errorf("the request failed after %d attempts, please try again later%s",
			numTries, errMsg)
	}
	return resp, fmt.Errorf("the request failed, please try again later%s", errMsg)
}

func requestLogHook(logger retryablehttp.Logger, req *http.Request, i int) {
	if i > 0 {
		logger.Printf("[INFO] Previous request to the remote registry failed, attempting retry.")
	}
}

// hostFromRequest extracts host the same way net/http Request.Write would,
// accounting for empty Request.Host
func hostFromRequest(req *http.Request) string {
	if req.Host != "" {
		return req.Host
	}
	if req.URL != nil {
		return req.URL.Host
	}

	// this should never happen and if it does
	// it will be handled as part of Request.Write()
	// https://cs.opensource.google/go/go/+/refs/tags/go1.18.4:src/net/http/request.go;l=574
	return ""
}
