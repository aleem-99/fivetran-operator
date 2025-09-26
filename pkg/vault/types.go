package vault

import (
	vault "github.com/hashicorp/vault/api"
)

// VaultClient wraps the Vault API client with its configuration
type VaultClient struct {
	Client *vault.Client
	Config *ClientConfig
}

// ClientConfig holds the configuration for creating a Vault client
type ClientConfig struct {
	Address   string
	RoleID    string
	SecretID  string
	MountPath string
}
