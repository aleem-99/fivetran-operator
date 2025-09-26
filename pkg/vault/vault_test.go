package vault

import (
	"context"
	"os"
	"testing"

	vaultapi "github.com/hashicorp/vault/api"
	vaulthttp "github.com/hashicorp/vault/http"
	"github.com/hashicorp/vault/vault"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func setupTestVault(t *testing.T) (*vaultapi.Client, string, func()) {
	t.Helper()

	// Set VAULT_SKIP_VERIFY for testing
	if err := os.Setenv("VAULT_SKIP_VERIFY", "true"); err != nil {
		t.Fatalf("failed to set VAULT_SKIP_VERIFY: %v", err)
	}

	cluster := vault.NewTestCluster(t, &vault.CoreConfig{
		DevToken: "test-token",
		LogLevel: "error",
	}, &vault.TestClusterOptions{
		HandlerFunc: vaulthttp.Handler,
		NumCores:    1,
	})
	cluster.Start()

	core := cluster.Cores[0].Core
	vault.TestWaitActive(t, core)
	vaultClient := cluster.Cores[0].Client

	// Enable AppRole auth method
	if err := vaultClient.Sys().EnableAuthWithOptions("approle", &vaultapi.EnableAuthOptions{
		Type: "approle",
	}); err != nil {
		t.Fatalf("failed to enable approle auth: %v", err)
	}

	// Create a test role
	roleData := map[string]interface{}{
		"token_ttl":     "1h",
		"token_max_ttl": "4h",
		"policies":      []string{"default"},
	}
	if _, err := vaultClient.Logical().Write("auth/approle/role/test-role", roleData); err != nil {
		t.Fatalf("failed to create test role: %v", err)
	}

	// Get role ID
	roleIDResp, err := vaultClient.Logical().Read("auth/approle/role/test-role/role-id")
	if err != nil {
		t.Fatalf("failed to read role ID: %v", err)
	}
	roleID := roleIDResp.Data["role_id"].(string)

	return vaultClient, roleID, func() {
		if err := os.Unsetenv("VAULT_SKIP_VERIFY"); err != nil {
			t.Logf("failed to unset VAULT_SKIP_VERIFY: %v", err)
		}
		cluster.Cleanup()
	}
}

func TestNewClient(t *testing.T) {
	testClient, roleID, cleanup := setupTestVault(t)
	defer cleanup()

	// Generate a valid secret ID for success case
	secretIDResp, err := testClient.Logical().Write("auth/approle/role/test-role/secret-id", nil)
	if err != nil {
		t.Fatalf("failed to generate secret ID: %v", err)
	}
	validSecretID := secretIDResp.Data["secret_id"].(string)

	tests := []struct {
		name        string
		config      *ClientConfig
		expectError bool
	}{
		{
			name: "valid configuration",
			config: &ClientConfig{
				Address:   testClient.Address(),
				RoleID:    roleID,
				SecretID:  validSecretID,
				MountPath: "apps",
			},
			expectError: false, // Should succeed with valid credentials
		},
		{
			name: "invalid secret ID",
			config: &ClientConfig{
				Address:   testClient.Address(),
				RoleID:    roleID,
				SecretID:  "invalid-secret-id",
				MountPath: "apps",
			},
			expectError: true,
		},
		{
			name: "empty role ID",
			config: &ClientConfig{
				Address:   testClient.Address(),
				RoleID:    "",
				SecretID:  validSecretID,
				MountPath: "apps",
			},
			expectError: true,
		},
		{
			name: "empty secret ID",
			config: &ClientConfig{
				Address:   testClient.Address(),
				RoleID:    roleID,
				SecretID:  "",
				MountPath: "apps",
			},
			expectError: true,
		},
		{
			name: "invalid vault address",
			config: &ClientConfig{
				Address:   "http://invalid-vault-address:8200",
				RoleID:    roleID,
				SecretID:  validSecretID,
				MountPath: "apps",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vaultClient, err := NewClient(tt.config)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if vaultClient == nil {
				t.Error("expected client but got nil")
				return
			}

			// For success cases, verify the client can make authenticated requests
			_, err = vaultClient.Auth().Token().LookupSelf()
			if err != nil {
				t.Errorf("failed to lookup self with authenticated client: %v", err)
			}
		})
	}
}

func TestIsTokenValid(t *testing.T) {
	testClient, roleID, cleanup := setupTestVault(t)
	defer cleanup()

	// Generate a valid secret ID
	secretIDResp, err := testClient.Logical().Write("auth/approle/role/test-role/secret-id", nil)
	if err != nil {
		t.Fatalf("failed to generate secret ID: %v", err)
	}
	secretID := secretIDResp.Data["secret_id"].(string)

	config := &ClientConfig{
		Address:   testClient.Address(),
		RoleID:    roleID,
		SecretID:  secretID,
		MountPath: "apps",
	}

	authenticatedClient, err := NewClient(config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	vaultClient := &VaultClient{
		Client: authenticatedClient,
		Config: config,
	}

	tests := []struct {
		name           string
		vaultClient    *VaultClient
		minTTLSeconds  int64
		expectedResult bool
	}{
		{
			name:           "valid token with sufficient TTL",
			vaultClient:    vaultClient,
			minTTLSeconds:  60, // 1 minute
			expectedResult: true,
		},
		{
			name:           "valid token with high TTL requirement",
			vaultClient:    vaultClient,
			minTTLSeconds:  7200, // 2 hours (should fail as token TTL is 1 hour)
			expectedResult: false,
		},
		{
			name:           "nil vault client",
			vaultClient:    nil,
			minTTLSeconds:  60,
			expectedResult: false,
		},
		{
			name: "vault client with nil client",
			vaultClient: &VaultClient{
				Client: nil,
				Config: config,
			},
			minTTLSeconds:  60,
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTokenValid(tt.vaultClient, tt.minTTLSeconds)
			if result != tt.expectedResult {
				t.Errorf("IsTokenValid() = %v, expected %v", result, tt.expectedResult)
			}
		})
	}
}

func TestNewClientConfig(t *testing.T) {
	tests := []struct {
		name        string
		address     string
		roleID      string
		secretID    string
		mountPath   string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid configuration",
			address:     "http://127.0.0.1:8200",
			roleID:      "test-role-id",
			secretID:    "test-secret-id",
			mountPath:   "apps",
			expectError: false,
		},
		{
			name:        "empty address",
			address:     "",
			roleID:      "test-role-id",
			secretID:    "test-secret-id",
			mountPath:   "apps",
			expectError: true,
			errorMsg:    "vault address is required",
		},
		{
			name:        "empty roleID",
			address:     "http://127.0.0.1:8200",
			roleID:      "",
			secretID:    "test-secret-id",
			mountPath:   "apps",
			expectError: true,
			errorMsg:    "vault roleID is required",
		},
		{
			name:        "empty secretID",
			address:     "http://127.0.0.1:8200",
			roleID:      "test-role-id",
			secretID:    "",
			mountPath:   "apps",
			expectError: true,
			errorMsg:    "vault secretID is required",
		},
		{
			name:        "empty mountPath",
			address:     "http://127.0.0.1:8200",
			roleID:      "test-role-id",
			secretID:    "test-secret-id",
			mountPath:   "",
			expectError: true,
			errorMsg:    "vault mountPath is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewClientConfig(tt.address, tt.roleID, tt.secretID, tt.mountPath)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
					return
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if config == nil {
				t.Error("expected config but got nil")
				return
			}

			// Verify all fields are set correctly
			if config.Address != tt.address {
				t.Errorf("expected address '%s', got '%s'", tt.address, config.Address)
			}
			if config.RoleID != tt.roleID {
				t.Errorf("expected roleID '%s', got '%s'", tt.roleID, config.RoleID)
			}
			if config.SecretID != tt.secretID {
				t.Errorf("expected secretID '%s', got '%s'", tt.secretID, config.SecretID)
			}
			if config.MountPath != tt.mountPath {
				t.Errorf("expected mountPath '%s', got '%s'", tt.mountPath, config.MountPath)
			}
		})
	}
}

func TestInitializeVaultClientFromSecret(t *testing.T) {
	testClient, roleID, cleanup := setupTestVault(t)
	defer cleanup()

	// Generate a valid secret ID
	secretIDResp, err := testClient.Logical().Write("auth/approle/role/test-role/secret-id", nil)
	if err != nil {
		t.Fatalf("failed to generate secret ID: %v", err)
	}
	secretID := secretIDResp.Data["secret_id"].(string)

	// Create test secrets
	validSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "valid-vault-secret",
			Namespace: "test-namespace",
		},
		Data: map[string][]byte{
			"address":   []byte(testClient.Address()),
			"roleId":    []byte(roleID),
			"secretId":  []byte(secretID),
			"mountPath": []byte("apps"),
		},
	}

	incompleteSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "incomplete-vault-secret",
			Namespace: "test-namespace",
		},
		Data: map[string][]byte{
			"address": []byte(testClient.Address()),
			"roleId":  []byte(roleID),
			// Missing secretId and mountPath
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name        string
		secretName  string
		namespace   string
		objects     []client.Object
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid secret",
			secretName:  "valid-vault-secret",
			namespace:   "test-namespace",
			objects:     []client.Object{validSecret},
			expectError: false,
		},
		{
			name:        "secret not found",
			secretName:  "nonexistent-secret",
			namespace:   "test-namespace",
			objects:     []client.Object{},
			expectError: true,
			errorMsg:    "secrets \"nonexistent-secret\" not found",
		},
		{
			name:        "incomplete secret data",
			secretName:  "incomplete-vault-secret",
			namespace:   "test-namespace",
			objects:     []client.Object{incompleteSecret},
			expectError: true,
			errorMsg:    "vault secretID is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.objects...).
				Build()

			vaultClient, err := InitializeVaultClientFromSecret(
				context.Background(),
				k8sClient,
				tt.namespace,
				tt.secretName,
			)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
					return
				}
				if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if vaultClient == nil {
				t.Error("expected vault client but got nil")
				return
			}

			if vaultClient.Client == nil {
				t.Error("expected vault client.Client but got nil")
			}

			if vaultClient.Config == nil {
				t.Error("expected vault client.Config but got nil")
			}
		})
	}
}
