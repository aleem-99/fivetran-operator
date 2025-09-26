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
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/redhat-data-and-ai/fivetran-operator/api/v1alpha1"
)

// reconcileSetupTests runs setup tests
func (r *FivetranConnectorReconciler) reconcileSetupTests(ctx context.Context, connector *operatorv1alpha1.FivetranConnector, connectorID string) ([]string, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling setup tests")
	// Check if tests are requested (default to true if nil)
	var setupTestErrors []error
	var warningMessages []string
	runTests := true
	if connector.Spec.Connector.RunSetupTests != nil {
		runTests = *connector.Spec.Connector.RunSetupTests
	}

	if !runTests {
		if err := r.updateSetupTestsCondition(ctx, connector, warningMessages); err != nil {
			return warningMessages, err
		}
		logger.Info("skipping setup tests", "connectorId", connectorID)
		return nil, nil
	}

	// Run setup tests
	logger.Info("Running setup tests", "connectorId", connectorID)
	resp, err := r.FivetranClient.Connections.RunSetupTests(ctx, connectorID, connector.Spec.Connector.TrustCertificates, connector.Spec.Connector.TrustFingerprints)
	if err != nil {
		return nil, fmt.Errorf("reconcileSetupTests: %w", err)
	}

	// Check test results

	for _, test := range resp.Data.SetupTests {
		// Only PASSED, SKIPPED, and WARNING are considered successful
		// FAILED and JOB_FAILED should be treated as failures
		logger.Info("Setup test result", "title", test.Title, "status", test.Status, "message", test.Message, "details", test.Details)
		if test.Status == setupTestStatusWarning {
			warningMessages = append(warningMessages, fmt.Sprintf("%s: %s", test.Title, test.Message))
		} else if test.Status != setupTestStatusPassed && test.Status != setupTestStatusSkipped {
			setupTestErrors = append(setupTestErrors, fmt.Errorf("reconcileSetupTests failed: %s (status: %s) - %s", test.Title, test.Status, test.Message))
		}
	}

	if len(setupTestErrors) > 0 {
		return warningMessages, fmt.Errorf("%w: %s", ErrSetupTestsFailed, errors.Join(setupTestErrors...).Error())
	}

	// Return warning messages (if any) and no error
	if len(warningMessages) > 0 {
		if err := r.updateSetupTestsCondition(ctx, connector, warningMessages); err != nil {
			return warningMessages, err
		}
		logger.Info("Setup tests completed with warnings", "connectorId", connectorID, "warnings", warningMessages)
	} else {
		if err := r.updateSetupTestsCondition(ctx, connector, warningMessages); err != nil {
			return warningMessages, err
		}
		logger.Info("Setup tests completed successfully", "connectorId", connectorID)
	}

	return warningMessages, nil
}
