package fivetran

import (
	"errors"
	"fmt"

	"github.com/fivetran/go-fivetran/connections"
)

// SchemaBuilder provides a fluent interface for building schema configurations
type SchemaBuilder struct {
	schemas              map[string]*connections.ConnectionSchemaConfigSchema
	schemaChangeHandling string
	err                  error
}

// NewSchemaBuilder creates a new SchemaBuilder instance
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		schemas: make(map[string]*connections.ConnectionSchemaConfigSchema),
	}
}

// WithSchemaChangeHandling sets the schema change handling policy
func (b *SchemaBuilder) WithSchemaChangeHandling(handling string) *SchemaBuilder {
	if b.err != nil {
		return b
	}
	b.schemaChangeHandling = handling
	return b
}

// AddSchema adds a schema configuration
func (b *SchemaBuilder) AddSchema(name string, enabled bool) *SchemaBuilder {
	if b.err != nil {
		return b
	}
	if name == "" {
		b.err = errors.New("schema name cannot be empty")
		return b
	}
	schema := &connections.ConnectionSchemaConfigSchema{}
	schema.Enabled(enabled)
	b.schemas[name] = schema
	return b
}

// AddTable adds a table configuration to a schema
func (b *SchemaBuilder) AddTable(schema, table string, enabled bool, syncMode string) *SchemaBuilder {
	if b.err != nil {
		return b
	}
	if schema == "" || table == "" {
		b.err = errors.New("schema and table names cannot be empty")
		return b
	}
	s, ok := b.schemas[schema]
	if !ok {
		b.err = fmt.Errorf("schema %q not found", schema)
		return b
	}

	tableConfig := &connections.ConnectionSchemaConfigTable{}
	tableConfig.Enabled(enabled)
	if syncMode != "" {
		tableConfig.SyncMode(syncMode)
	}
	s.Table(table, tableConfig)
	return b
}

// AddColumn adds a column configuration to a table
func (b *SchemaBuilder) AddColumn(schema, table, column string, enabled, hashed, isPrimaryKey bool) *SchemaBuilder {
	if b.err != nil {
		return b
	}
	if schema == "" || table == "" || column == "" {
		b.err = errors.New("schema, table, and column names cannot be empty")
		return b
	}

	s, ok := b.schemas[schema]
	if !ok {
		b.err = fmt.Errorf("schema %q not found", schema)
		return b
	}

	columnConfig := &connections.ConnectionSchemaConfigColumn{}
	columnConfig.Enabled(enabled)
	columnConfig.Hashed(hashed)
	columnConfig.IsPrimaryKey(isPrimaryKey)

	tableConfig := &connections.ConnectionSchemaConfigTable{}
	s.Table(table, tableConfig)
	tableConfig.Column(column, columnConfig)

	return b
}

// Build returns the final schema configuration
func (b *SchemaBuilder) Build() (map[string]*connections.ConnectionSchemaConfigSchema, string, error) {
	if b.err != nil {
		return nil, "", b.err
	}
	return b.schemas, b.schemaChangeHandling, nil
}
