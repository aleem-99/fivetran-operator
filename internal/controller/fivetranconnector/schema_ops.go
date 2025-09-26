/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fivetranconnector

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/redhat-data-and-ai/fivetran-operator/api/v1alpha1"
	"github.com/redhat-data-and-ai/fivetran-operator/internal/kubeutils"
	"github.com/redhat-data-and-ai/fivetran-operator/pkg/fivetran"
)

// reconcileSchema configures connector schema
func (r *FivetranConnectorReconciler) reconcileSchema(ctx context.Context, connector *operatorv1alpha1.FivetranConnector, connectorID string) error {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling schema")

	// Get current schema from Fivetran
	schemaDetails, err := r.FivetranClient.Schemas.GetSchemaDetails(ctx, connectorID)
	if err != nil {
		// Check if schema doesn't exist
		if schemaDetails.Code != SchemaNotFoundError {
			// Other error
			return fmt.Errorf("reconcileSchema: failed to get schema details: %w", err)
		}
		// reload schema to create it
		if err := r.reloadSchema(ctx, connector, connectorID); err != nil {
			return fmt.Errorf("reconcileSchema: %w", err)
		}
		logger.Info("Schema created successfully after reload", "connectorId", connectorID)
	}

	// Apply schema configuration
	if err := r.applySchema(ctx, connector, connectorID); err != nil {
		return fmt.Errorf("reconcileSchema: %w", err)
	}

	// Verify schema was applied correctly by fetching and comparing again
	logger.Info("Verifying schema was applied correctly by fetching and comparing again")
	schemaDetails, err = r.FivetranClient.Schemas.GetSchemaDetails(ctx, connectorID)
	if err != nil {
		return fmt.Errorf("reconcileSchema: failed to get schema details after apply: %w", err)
	}

	matches, mismatchDetails := fivetran.CompareSchemaWithCR(schemaDetails, connector.Spec.ConnectorSchemas)
	if !matches {
		logger.Info("Schema configuration doesn't match with the source, retrying once more",
			"connectorId", connectorID,
			"mismatches", mismatchDetails.String())

		// Reload schema and apply
		logger.Info("Reloading schema")
		if err := r.reloadSchema(ctx, connector, connectorID); err != nil {
			return fmt.Errorf("reconcileSchema reloadSchema retry: %w", err)
		}

		if err := r.applySchema(ctx, connector, connectorID); err != nil {
			return fmt.Errorf("reconcileSchema applySchema retry: %w", err)
		}

		// Final verification after retry
		schemaDetails, err = r.FivetranClient.Schemas.GetSchemaDetails(ctx, connectorID)
		if err != nil {
			return fmt.Errorf("reconcileSchema getSchemaDetails retry: %w", err)
		}

		retryMatches, retryMismatchDetails := fivetran.CompareSchemaWithCR(schemaDetails, connector.Spec.ConnectorSchemas)
		if !retryMatches {
			return fmt.Errorf("reconcileSchema compareSchemaWithCR retry: mismatches: %s - %w", retryMismatchDetails.String(), ErrSchemaMismatchAfterRetry)
		}
	}

	if err := r.setCondition(ctx, connector, conditionTypeSchemaReady, metav1.ConditionTrue, SchemaReasonReconciliationSuccess, msgSchemaReady); err != nil {
		return err
	}
	logger.Info("Schema configuration applied successfully", "connectorId", connectorID)

	return nil
}

// reloadSchema will create a schema if it doesn't exist or reloads it if it does
func (r *FivetranConnectorReconciler) reloadSchema(ctx context.Context, connector *operatorv1alpha1.FivetranConnector, connectorID string) error {
	logger := log.FromContext(ctx)

	excludeMode := "PRESERVE"
	if connector.Spec.ConnectorSchemas.SchemaChangeHandling == "BLOCK_ALL" {
		excludeMode = "EXCLUDE"
	}

	logger.Info("Reloading schema", "connectorId", connectorID, "excludeMode", excludeMode)
	_, err := r.FivetranClient.Schemas.ReloadSchema(ctx, connectorID, excludeMode)
	if err != nil {
		return fmt.Errorf("reloadSchema: %w", err)
	}

	logger.Info("Schema reloaded successfully", "connectorId", connectorID, "excludeMode", excludeMode)
	return nil
}

// applySchema applies schema configuration
func (r *FivetranConnectorReconciler) applySchema(ctx context.Context, connector *operatorv1alpha1.FivetranConnector, connectorID string) error {
	logger := log.FromContext(ctx)
	logger.Info("Applying schema configuration", "connectorId", connectorID)
	schema := r.convertSchema(connector.Spec.ConnectorSchemas)

	_, err := r.FivetranClient.Schemas.UpdateSchema(ctx, connectorID, schema)
	if err != nil {
		return fmt.Errorf("applySchema: %w", err)
	}

	return r.updateSchemaHash(ctx, connector)
}

// updateSchemaHash updates only the schema hash annotation
func (r *FivetranConnectorReconciler) updateSchemaHash(ctx context.Context, connector *operatorv1alpha1.FivetranConnector) error {
	hash, err := r.calculateSchemaHash(connector)
	if err != nil {
		return err
	}
	kubeutils.SetAnnotation(connector, annotationSchemaHash, hash)
	return r.Update(ctx, connector)
}
