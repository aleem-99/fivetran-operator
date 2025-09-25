package fivetran

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/fivetran/go-fivetran/common"
)

// APIError represents a Fivetran API error with status code and details
type APIError struct {
	StatusCode int
	Code       string // From CommonResponse.Code
	Message    string // From CommonResponse.Message
	RawError   string // Original error string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("fivetran api error (status %d): %s - %s", e.StatusCode, e.Code, e.Message)
}

// IsRetryable determines if a Fivetran error should be retried
func (e *APIError) IsRetryable() bool {
	switch e.StatusCode {
	case http.StatusTooManyRequests: // 429
		return true
	case http.StatusInternalServerError, // 500
		http.StatusBadGateway,         // 502
		http.StatusServiceUnavailable, // 503
		http.StatusGatewayTimeout:     // 504
		return true
	default:
		// 4xx errors (except 429) are generally not retryable
		return e.StatusCode < 400 || e.StatusCode >= 500
	}
}

// IsRetryableError is a convenience function to check if any error is retryable
func IsRetryableError(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable()
	}

	// For non-API errors (network, timeout, etc.), assume retryable
	return true
}

// AsAPIError checks if an error is an APIError and returns it
func AsAPIError(err error) (*APIError, bool) {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr, true
	}
	return nil, false
}

// WrapFivetranError creates an APIError from a response and error
// The Fivetran Go SDK only returns basic error information in the format "status code: %d; expected: %d"
// To get the actual error message and code, we need to extract detailed error information from the response's CommonResponse
func WrapFivetranError(response any, err error) error {
	if err == nil {
		return nil
	}

	apiErr := &APIError{
		RawError:   err.Error(),
		StatusCode: 0,
		Code:       "",
		Message:    "",
	}

	// Try to parse status code from the error string
	var code, expected int
	if _, scanErr := fmt.Sscanf(err.Error(), "status code: %d; expected: %d", &code, &expected); scanErr == nil {
		apiErr.StatusCode = code
	}

	// Try to extract error details from the response if it contains CommonResponse
	if response != nil {
		if commonResp, ok := extractCommonResponse(response); ok {
			apiErr.Code = commonResp.Code
			apiErr.Message = commonResp.Message
		}
	}

	return apiErr
}

// extractCommonResponse attempts to extract CommonResponse from various response types
func extractCommonResponse(response any) (*common.CommonResponse, bool) {
	// Handle direct CommonResponse
	if commonResp, ok := response.(*common.CommonResponse); ok {
		return commonResp, true
	}

	// Handle structs that embed CommonResponse
	// Use reflection to check if the response has a CommonResponse field
	if respBytes, err := json.Marshal(response); err == nil {
		var commonResp common.CommonResponse
		if err := json.Unmarshal(respBytes, &commonResp); err == nil {
			// Only consider it valid if we actually have error details
			if commonResp.Code != "" || commonResp.Message != "" {
				return &commonResp, true
			}
		}
	}

	return nil, false
}
