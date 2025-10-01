package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"
)

// DokitoClient handles communication with the dokito-backend service
type DokitoClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewDokitoClient creates a new dokito backend client
func NewDokitoClient() *DokitoClient {
	baseURL := os.Getenv("DOKITO_BACKEND_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8123"
	}

	return &DokitoClient{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// HealthCheck checks if the dokito backend is healthy
func (d *DokitoClient) HealthCheck() error {
	resp, err := d.HTTPClient.Get(d.BaseURL + "/health")
	if err != nil {
		return fmt.Errorf("failed to connect to dokito backend: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("dokito backend health check failed with status: %d", resp.StatusCode)
	}

	return nil
}

// makeRequest performs an HTTP request to the dokito backend
func (d *DokitoClient) makeRequest(ctx context.Context, method, endpoint string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
		log.Printf("Dokito request body: %s", string(jsonData))
	}

	req, err := http.NewRequestWithContext(ctx, method, d.BaseURL+endpoint, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	log.Printf("Making %s request to dokito backend: %s", method, d.BaseURL+endpoint)

	resp, err := d.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	log.Printf("Dokito response status: %d", resp.StatusCode)
	return resp, nil
}

// parseResponse parses the HTTP response into the target struct
func (d *DokitoClient) parseResponse(resp *http.Response, target interface{}) error {
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	log.Printf("Dokito response body: %s", string(body))

	if resp.StatusCode >= 400 {
		return fmt.Errorf("dokito backend error (status %d): %s", resp.StatusCode, string(body))
	}

	if target != nil {
		if err := json.Unmarshal(body, target); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}

	return nil
}

// SubmitRawDockets submits raw dockets for processing
func (d *DokitoClient) SubmitRawDockets(ctx context.Context, state, jurisdiction string, request *RawDocketsRequest) (*ProcessingResponse, error) {
	endpoint := fmt.Sprintf("/admin/docket-process/%s/%s/raw-dockets",
		url.PathEscape(state), url.PathEscape(jurisdiction))

	resp, err := d.makeRequest(ctx, "POST", endpoint, request)
	if err != nil {
		return nil, err
	}

	var result ProcessingResponse
	if err := d.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ProcessByIds processes dockets by their IDs
func (d *DokitoClient) ProcessByIds(ctx context.Context, state, jurisdiction string, request *ByIdsRequest) (*ProcessingResponse, error) {
	endpoint := fmt.Sprintf("/admin/docket-process/%s/%s/by-ids",
		url.PathEscape(state), url.PathEscape(jurisdiction))

	resp, err := d.makeRequest(ctx, "POST", endpoint, request)
	if err != nil {
		return nil, err
	}

	var result ProcessingResponse
	if err := d.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ProcessByJurisdiction processes all dockets for a jurisdiction
func (d *DokitoClient) ProcessByJurisdiction(ctx context.Context, state, jurisdiction string, request *ByJurisdictionRequest) (*ProcessingResponse, error) {
	endpoint := fmt.Sprintf("/admin/docket-process/%s/%s/by-jurisdiction",
		url.PathEscape(state), url.PathEscape(jurisdiction))

	resp, err := d.makeRequest(ctx, "POST", endpoint, request)
	if err != nil {
		return nil, err
	}

	var result ProcessingResponse
	if err := d.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ProcessByDateRange processes dockets within a date range
func (d *DokitoClient) ProcessByDateRange(ctx context.Context, state, jurisdiction string, request *ByDateRangeRequest) (*ProcessingResponse, error) {
	endpoint := fmt.Sprintf("/admin/docket-process/%s/%s/by-daterange",
		url.PathEscape(state), url.PathEscape(jurisdiction))

	resp, err := d.makeRequest(ctx, "POST", endpoint, request)
	if err != nil {
		return nil, err
	}

	var result ProcessingResponse
	if err := d.parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
