package fivetran

import (
	"context"
	"fmt"

	fivetran "github.com/fivetran/go-fivetran"
	"github.com/fivetran/go-fivetran/connections"
)

type schemaServiceImpl struct {
	client *fivetran.Client
}

func newSchemaService(client *fivetran.Client) SchemaService {
	return &schemaServiceImpl{client: client}
}

// CreateSchema configures the schema for a Connection
func (s *schemaServiceImpl) CreateSchema(ctx context.Context, ConnectionID string, builder *SchemaBuilder) (connections.ConnectionSchemaDetailsResponse, error) {
	schemas, schemaChangeHandling, err := builder.Build()
	if err != nil {
		return connections.ConnectionSchemaDetailsResponse{}, fmt.Errorf("failed to build schema config: %w", err)
	}

	schemaService := s.client.NewConnectionSchemaCreateService()
	service := schemaService.ConnectionID(ConnectionID)

	// Only set SchemaChangeHandling if it's provided
	if schemaChangeHandling != "" {
		service = service.SchemaChangeHandling(schemaChangeHandling)
	}

	// Only add schemas if they exist
	for schemaName, schema := range schemas {
		service = service.Schema(schemaName, schema)
	}

	resp, err := service.Do(ctx)

	return resp, WrapFivetranError(resp, err)
}

// UpdateSchema updates the schema configuration for a Connection
func (s *schemaServiceImpl) UpdateSchema(ctx context.Context, ConnectionID string, builder *SchemaBuilder) (connections.ConnectionSchemaDetailsResponse, error) {
	schemas, schemaChangeHandling, err := builder.Build()
	if err != nil {
		return connections.ConnectionSchemaDetailsResponse{}, fmt.Errorf("failed to build schema config: %w", err)
	}

	schemaService := s.client.NewConnectionSchemaUpdateService()
	service := schemaService.ConnectionID(ConnectionID)

	// Only set SchemaChangeHandling if it's provided
	if schemaChangeHandling != "" {
		service = service.SchemaChangeHandling(schemaChangeHandling)
	}

	// Only add schemas if they exist
	for schemaName, schema := range schemas {
		service = service.Schema(schemaName, schema)
	}

	resp, err := service.Do(ctx)
	return resp, WrapFivetranError(resp, err)
}

// GetSchemaDetails retrieves schema configuration details for a Connection
func (s *schemaServiceImpl) GetSchemaDetails(ctx context.Context, ConnectionID string) (connections.ConnectionSchemaDetailsResponse, error) {
	schemaService := s.client.NewConnectionSchemaDetails()
	resp, err := schemaService.ConnectionID(ConnectionID).Do(ctx)
	return resp, WrapFivetranError(resp, err)
}

// ReloadSchema reloads the schema configuration for a Connection
func (s *schemaServiceImpl) ReloadSchema(ctx context.Context, ConnectionID string, excludeMode string) (connections.ConnectionSchemaDetailsResponse, error) {
	reloadService := s.client.NewConnectionSchemaReload()
	resp, err := reloadService.
		ConnectionID(ConnectionID).
		ExcludeMode(excludeMode).
		Do(ctx)
	return resp, WrapFivetranError(resp, err)
}
