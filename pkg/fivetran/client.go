package fivetran

import (
	"errors"

	fivetran "github.com/fivetran/go-fivetran"
)

// Client manages the Fivetran API client and services
type Client struct {
	sdk         *fivetran.Client
	Connections ConnectorService
	Schemas     SchemaService
}

// NewClient creates a new Fivetran client with all services
func NewClient(apiKey, apiSecret string) (*Client, error) {
	if apiKey == "" || apiSecret == "" {
		return nil, errors.New("FIVETRAN_API_KEY and FIVETRAN_API_SECRET are required")
	}

	sdk := fivetran.New(apiKey, apiSecret)
	client := &Client{sdk: sdk}

	// Initialize services
	client.Connections = newConnectionService(sdk)
	client.Schemas = newSchemaService(sdk)

	return client, nil
}
