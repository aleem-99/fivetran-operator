package fivetran

import (
	"testing"

	"github.com/fivetran/go-fivetran/connections"
	operatorv1alpha1 "github.com/redhat-data-and-ai/fivetran-operator/api/v1alpha1"
)

// Helper function to create test schema response
func createSchemaResponse(schemas map[string]*connections.ConnectionSchemaConfigSchemaResponse) connections.ConnectionSchemaDetailsResponse {
	return connections.ConnectionSchemaDetailsResponse{
		Data: struct {
			SchemaChangeHandling string                                                       `json:"schema_change_handling"`
			Schemas              map[string]*connections.ConnectionSchemaConfigSchemaResponse `json:"schemas"`
		}{
			SchemaChangeHandling: "ALLOW_ALL",
			Schemas:              schemas,
		},
	}
}

func TestCompareSchemaWithCR(t *testing.T) {
	tests := []struct {
		name           string
		fivetranSchema connections.ConnectionSchemaDetailsResponse
		crSchema       *operatorv1alpha1.ConnectorSchemaConfig
		expectMatch    bool
		expectError    string
	}{
		{
			name:           "nil CR schema should match",
			fivetranSchema: connections.ConnectionSchemaDetailsResponse{},
			crSchema:       nil,
			expectMatch:    true,
		},
		{
			name:           "empty schemas should match",
			fivetranSchema: createSchemaResponse(make(map[string]*connections.ConnectionSchemaConfigSchemaResponse)),
			crSchema: &operatorv1alpha1.ConnectorSchemaConfig{
				SchemaChangeHandling: "ALLOW_ALL",
				Schemas:              make(map[string]*operatorv1alpha1.SchemaObject),
			},
			expectMatch: true,
		},
		{
			name:           "schema change handling mismatch",
			fivetranSchema: createSchemaResponse(make(map[string]*connections.ConnectionSchemaConfigSchemaResponse)),
			crSchema: &operatorv1alpha1.ConnectorSchemaConfig{
				SchemaChangeHandling: "BLOCK_ALL",
				Schemas:              make(map[string]*operatorv1alpha1.SchemaObject),
			},
			expectMatch: false,
			expectError: "expected BLOCK_ALL, got ALLOW_ALL",
		},
		{
			name:           "missing schema in Fivetran",
			fivetranSchema: createSchemaResponse(make(map[string]*connections.ConnectionSchemaConfigSchemaResponse)),
			crSchema: &operatorv1alpha1.ConnectorSchemaConfig{
				SchemaChangeHandling: "ALLOW_ALL",
				Schemas: map[string]*operatorv1alpha1.SchemaObject{
					"test_schema": {
						Enabled: true,
					},
				},
			},
			expectMatch: false,
			expectError: "test_schema",
		},
		{
			name: "schema enabled state mismatch",
			fivetranSchema: createSchemaResponse(map[string]*connections.ConnectionSchemaConfigSchemaResponse{
				"test_schema": {
					Enabled: boolPtr(false),
					Tables:  make(map[string]*connections.ConnectionSchemaConfigTableResponse),
				},
			}),
			crSchema: &operatorv1alpha1.ConnectorSchemaConfig{
				SchemaChangeHandling: "ALLOW_ALL",
				Schemas: map[string]*operatorv1alpha1.SchemaObject{
					"test_schema": {
						Enabled: true,
					},
				},
			},
			expectMatch: false,
			expectError: "expected true, got false",
		},
		{
			name: "missing table in Fivetran",
			fivetranSchema: createSchemaResponse(map[string]*connections.ConnectionSchemaConfigSchemaResponse{
				"test_schema": {
					Enabled: boolPtr(true),
					Tables:  make(map[string]*connections.ConnectionSchemaConfigTableResponse),
				},
			}),
			crSchema: &operatorv1alpha1.ConnectorSchemaConfig{
				SchemaChangeHandling: "ALLOW_ALL",
				Schemas: map[string]*operatorv1alpha1.SchemaObject{
					"test_schema": {
						Enabled: true,
						Tables: map[string]*operatorv1alpha1.TableObject{
							"test_table": {
								Enabled: true,
							},
						},
					},
				},
			},
			expectMatch: false,
			expectError: "not found in source",
		},
		{
			name: "table enabled state mismatch",
			fivetranSchema: createSchemaResponse(map[string]*connections.ConnectionSchemaConfigSchemaResponse{
				"test_schema": {
					Enabled: boolPtr(true),
					Tables: map[string]*connections.ConnectionSchemaConfigTableResponse{
						"test_table": {
							Enabled: boolPtr(false),
						},
					},
				},
			}),
			crSchema: &operatorv1alpha1.ConnectorSchemaConfig{
				SchemaChangeHandling: "ALLOW_ALL",
				Schemas: map[string]*operatorv1alpha1.SchemaObject{
					"test_schema": {
						Enabled: true,
						Tables: map[string]*operatorv1alpha1.TableObject{
							"test_table": {
								Enabled: true,
							},
						},
					},
				},
			},
			expectMatch: false,
			expectError: "enabled state mismatch: expected true, got false",
		},
		{
			name: "table sync mode mismatch - nil in Fivetran",
			fivetranSchema: createSchemaResponse(map[string]*connections.ConnectionSchemaConfigSchemaResponse{
				"test_schema": {
					Enabled: boolPtr(true),
					Tables: map[string]*connections.ConnectionSchemaConfigTableResponse{
						"test_table": {
							Enabled:  boolPtr(true),
							SyncMode: nil,
						},
					},
				},
			}),
			crSchema: &operatorv1alpha1.ConnectorSchemaConfig{
				SchemaChangeHandling: "ALLOW_ALL",
				Schemas: map[string]*operatorv1alpha1.SchemaObject{
					"test_schema": {
						Enabled: true,
						Tables: map[string]*operatorv1alpha1.TableObject{
							"test_table": {
								Enabled:  true,
								SyncMode: "SOFT_DELETE",
							},
						},
					},
				},
			},
			expectMatch: false,
			expectError: "expected SOFT_DELETE, got nil",
		},
		{
			name: "table sync mode mismatch - different values",
			fivetranSchema: createSchemaResponse(map[string]*connections.ConnectionSchemaConfigSchemaResponse{
				"test_schema": {
					Enabled: boolPtr(true),
					Tables: map[string]*connections.ConnectionSchemaConfigTableResponse{
						"test_table": {
							Enabled:  boolPtr(true),
							SyncMode: stringPtr("HISTORY"),
						},
					},
				},
			}),
			crSchema: &operatorv1alpha1.ConnectorSchemaConfig{
				SchemaChangeHandling: "ALLOW_ALL",
				Schemas: map[string]*operatorv1alpha1.SchemaObject{
					"test_schema": {
						Enabled: true,
						Tables: map[string]*operatorv1alpha1.TableObject{
							"test_table": {
								Enabled:  true,
								SyncMode: "SOFT_DELETE",
							},
						},
					},
				},
			},
			expectMatch: false,
			expectError: "expected SOFT_DELETE, got HISTORY",
		},
		{
			name: "perfect match should pass",
			fivetranSchema: createSchemaResponse(map[string]*connections.ConnectionSchemaConfigSchemaResponse{
				"test_schema": {
					Enabled: boolPtr(true),
					Tables: map[string]*connections.ConnectionSchemaConfigTableResponse{
						"test_table": {
							Enabled:  boolPtr(true),
							SyncMode: stringPtr("SOFT_DELETE"),
						},
					},
				},
			}),
			crSchema: &operatorv1alpha1.ConnectorSchemaConfig{
				SchemaChangeHandling: "ALLOW_ALL",
				Schemas: map[string]*operatorv1alpha1.SchemaObject{
					"test_schema": {
						Enabled: true,
						Tables: map[string]*operatorv1alpha1.TableObject{
							"test_table": {
								Enabled:  true,
								SyncMode: "SOFT_DELETE",
							},
						},
					},
				},
			},
			expectMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches, mismatch := CompareSchemaWithCR(tt.fivetranSchema, tt.crSchema)

			if matches != tt.expectMatch {
				t.Errorf("CompareSchemaWithCR() matches = %v, want %v", matches, tt.expectMatch)
			}

			if tt.expectMatch {
				if mismatch.HasMismatch {
					t.Errorf("Expected no mismatch, but got: %s", mismatch.String())
				}
			} else {
				if !mismatch.HasMismatch {
					t.Errorf("Expected mismatch, but got none")
				}
				if tt.expectError != "" {
					mismatchStr := mismatch.String()
					if !contains(mismatchStr, tt.expectError) {
						t.Errorf("Expected error to contain '%s', got: %s", tt.expectError, mismatchStr)
					}
				}
			}
		})
	}
}

// Helper functions
func boolPtr(b bool) *bool {
	return &b
}

func stringPtr(s string) *string {
	return &s
}

func contains(str, substr string) bool {
	return len(str) >= len(substr) &&
		(len(substr) == 0 ||
			str[:len(substr)] == substr ||
			str[len(str)-len(substr):] == substr ||
			containsSubstring(str, substr))
}

func containsSubstring(str, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
