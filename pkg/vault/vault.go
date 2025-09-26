package vault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	vault "github.com/hashicorp/vault/api"
	auth "github.com/hashicorp/vault/api/auth/approle"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewClient creates a new vault client
func NewClient(cfg *ClientConfig) (*vault.Client, error) {
	config := vault.DefaultConfig()
	config.Address = cfg.Address
	vaultClient, err := vault.NewClient(config)
	if err != nil {
		return nil, err
	}

	appRoleAuth, err := auth.NewAppRoleAuth(
		cfg.RoleID,
		&auth.SecretID{FromString: cfg.SecretID},
	)
	if err != nil {
		return nil, err
	}

	authInfo, err := vaultClient.Auth().Login(context.Background(), appRoleAuth)
	if err != nil {
		return nil, err
	}
	if authInfo == nil {
		return nil, fmt.Errorf("no auth info was returned after login")
	}

	return vaultClient, nil
}

// IsTokenValid checks if the token is valid and has a TTL greater than the minimum TTL
func IsTokenValid(vc *VaultClient, minTTLSeconds int64) bool {
	if vc == nil || vc.Client == nil {
		return false
	}

	resp, err := vc.Client.Auth().Token().LookupSelf()
	if err != nil {
		return false
	}

	ttlRaw, exists := resp.Data["ttl"]
	if !exists {
		return false
	}

	ttlJSONNumber, ok := ttlRaw.(json.Number)
	if !ok {
		return false
	}

	ttlSeconds, err := ttlJSONNumber.Int64()
	if err != nil {
		return false
	}

	if ttlSeconds <= minTTLSeconds {
		return false
	}

	return true
}

// NewClientConfig creates a new ClientConfig with the provided address and AppRole credentials
func NewClientConfig(address, roleID, secretID, mountPath string) (*ClientConfig, error) {
	if address == "" {
		return nil, errors.New("vault address is required")
	}
	if roleID == "" {
		return nil, errors.New("vault roleID is required")
	}
	if secretID == "" {
		return nil, errors.New("vault secretID is required")
	}
	if mountPath == "" {
		return nil, errors.New("vault mountPath is required")
	}

	clientConfig := &ClientConfig{
		Address:   address,
		RoleID:    roleID,
		SecretID:  secretID,
		MountPath: mountPath,
	}
	return clientConfig, nil
}

// InitializeVaultClientFromSecret creates and authenticates a new Vault client using credentials
// stored in a Kubernetes secret.
func InitializeVaultClientFromSecret(ctx context.Context, k8sClient client.Client, namespace, secretName string) (*VaultClient, error) {
	vaultSecret := &corev1.Secret{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, vaultSecret); err != nil {
		return nil, err
	}

	vaultConfig, err := NewClientConfig(
		string(vaultSecret.Data["address"]),
		string(vaultSecret.Data["roleId"]),
		string(vaultSecret.Data["secretId"]),
		string(vaultSecret.Data["mountPath"]),
	)
	if err != nil {
		return nil, err
	}

	vaultClient, err := NewClient(vaultConfig)
	if err != nil {
		return nil, err
	}

	return &VaultClient{
		Client: vaultClient,
		Config: vaultConfig,
	}, nil
}
