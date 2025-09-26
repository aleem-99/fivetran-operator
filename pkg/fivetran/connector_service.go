package fivetran

import (
	"context"

	fivetran "github.com/fivetran/go-fivetran"
	"github.com/fivetran/go-fivetran/common"
	"github.com/fivetran/go-fivetran/connections"
)

type connectionServiceImpl struct {
	client *fivetran.Client
}

func newConnectionService(client *fivetran.Client) ConnectorService {
	return &connectionServiceImpl{client: client}
}

// Connection represents a Fivetran Connection configuration
type Connector struct {
	Service                 string          `json:"service"`
	Config                  *map[string]any `json:"config"`
	Auth                    *map[string]any `json:"auth"`
	Paused                  *bool           `json:"paused"`
	GroupID                 string          `json:"group_id"`
	SyncFrequency           int             `json:"sync_frequency"`
	DailySyncTime           string          `json:"daily_sync_time"`
	RunSetupTests           *bool           `json:"run_setup_tests"`
	ScheduleType            string          `json:"schedule_type"`
	PauseAfterTrial         *bool           `json:"pause_after_trial"`
	TrustCertificates       *bool           `json:"trust_certificates"`
	TrustFingerprints       *bool           `json:"trust_fingerprints"`
	DataDelaySensitivity    string          `json:"data_delay_sensitivity,omitempty"`
	DataDelayThreshold      int             `json:"data_delay_threshold,omitempty"`
	NetworkingMethod        string          `json:"networking_method,omitempty"`
	ProxyAgentID            string          `json:"proxy_agent_id,omitempty"`
	PrivateLinkID           string          `json:"private_link_id,omitempty"`
	HybridDeploymentAgentID string          `json:"hybrid_deployment_agent_id,omitempty"`
}

// CreateConnection creates a new Fivetran Connection
func (s *connectionServiceImpl) CreateConnection(ctx context.Context, Connection *Connector) (connections.DetailsWithCustomConfigResponse, error) {
	ConnectionService := s.client.NewConnectionCreate()

	service := ConnectionService.
		Service(Connection.Service).
		GroupID(Connection.GroupID).
		RunSetupTests(false)

	// Handle pointer fields with nil checks
	if Connection.Paused != nil {
		service = service.Paused(*Connection.Paused)
	}

	if Connection.SyncFrequency != 0 {
		service = service.SyncFrequency(&Connection.SyncFrequency)
	}

	if Connection.DailySyncTime != "" {
		service = service.DailySyncTime(Connection.DailySyncTime)
	}

	// Note: ScheduleType is not available in fivetran go sdk

	if Connection.PauseAfterTrial != nil {
		service = service.PauseAfterTrial(*Connection.PauseAfterTrial)
	}

	if Connection.Config != nil {
		service = service.ConfigCustom(Connection.Config)
	}

	if Connection.Auth != nil {
		service = service.AuthCustom(Connection.Auth)
	}

	if Connection.NetworkingMethod != "" {
		service = service.NetworkingMethod(Connection.NetworkingMethod)
	}

	if Connection.ProxyAgentID != "" {
		service = service.ProxyAgentId(Connection.ProxyAgentID)
	}

	if Connection.PrivateLinkID != "" {
		service = service.PrivateLinkId(Connection.PrivateLinkID)
	}

	if Connection.HybridDeploymentAgentID != "" {
		service = service.HybridDeploymentAgentId(Connection.HybridDeploymentAgentID)
	}

	if Connection.DataDelaySensitivity != "" {
		service = service.DataDelaySensitivity(Connection.DataDelaySensitivity)
	}

	if Connection.DataDelayThreshold != 0 {
		service = service.DataDelayThreshold(&Connection.DataDelayThreshold)
	}

	resp, err := service.DoCustom(ctx)
	return resp, WrapFivetranError(resp, err)
}

// GetConnection retrieves a Fivetran Connection by ID
func (s *connectionServiceImpl) GetConnection(ctx context.Context, ConnectionID string) (connections.DetailsWithCustomConfigNoTestsResponse, error) {
	ConnectionService := s.client.NewConnectionDetails()
	resp, err := ConnectionService.ConnectionID(ConnectionID).DoCustom(ctx)
	return resp, WrapFivetranError(resp, err)
}

// UpdateConnection updates an existing Fivetran Connection
func (s *connectionServiceImpl) UpdateConnection(ctx context.Context, ConnectionID string, Connection *Connector) (connections.DetailsWithCustomConfigResponse, error) {
	ConnectionService := s.client.NewConnectionUpdate()
	service := ConnectionService.ConnectionID(ConnectionID).RunSetupTests(false)

	// Handle pointer fields with nil checks
	if Connection.Paused != nil {
		service = service.Paused(*Connection.Paused)
	}

	if Connection.SyncFrequency != 0 {
		service = service.SyncFrequency(&Connection.SyncFrequency)
	}

	if Connection.DailySyncTime != "" {
		service = service.DailySyncTime(Connection.DailySyncTime)
	}

	if Connection.ScheduleType != "" {
		service = service.ScheduleType(Connection.ScheduleType)
	}

	if Connection.PauseAfterTrial != nil {
		service = service.PauseAfterTrial(*Connection.PauseAfterTrial)
	}

	if Connection.Config != nil {
		service = service.ConfigCustom(Connection.Config)
	}

	if Connection.Auth != nil {
		service = service.AuthCustom(Connection.Auth)
	}

	if Connection.NetworkingMethod != "" {
		service = service.NetworkingMethod(Connection.NetworkingMethod)
	}

	if Connection.ProxyAgentID != "" {
		service = service.ProxyAgentId(Connection.ProxyAgentID)
	}

	if Connection.PrivateLinkID != "" {
		service = service.PrivateLinkId(Connection.PrivateLinkID)
	}

	if Connection.HybridDeploymentAgentID != "" {
		service = service.HybridDeploymentAgentId(Connection.HybridDeploymentAgentID)
	}

	if Connection.DataDelaySensitivity != "" {
		service = service.DataDelaySensitivity(Connection.DataDelaySensitivity)
	}

	if Connection.DataDelayThreshold != 0 {
		service = service.DataDelayThreshold(&Connection.DataDelayThreshold)
	}

	resp, err := service.DoCustom(ctx)
	return resp, WrapFivetranError(resp, err)
}

// DeleteConnection deletes a Fivetran Connection
func (s *connectionServiceImpl) DeleteConnection(ctx context.Context, ConnectionID string) (common.CommonResponse, error) {
	ConnectionService := s.client.NewConnectionDelete()
	resp, err := ConnectionService.ConnectionID(ConnectionID).Do(ctx)
	return resp, WrapFivetranError(resp, err)
}

// RunSetupTests runs setup tests for a Connection
func (s *connectionServiceImpl) RunSetupTests(ctx context.Context, ConnectionID string, trustCertificates, trustFingerprints *bool) (connections.DetailsWithConfigResponse, error) {
	testService := s.client.NewConnectionSetupTests()
	service := testService.ConnectionID(ConnectionID)

	if trustCertificates != nil {
		service = service.TrustCertificates(*trustCertificates)
	} else {
		service = service.TrustCertificates(true) // Default to true
	}

	if trustFingerprints != nil {
		service = service.TrustFingerprints(*trustFingerprints)
	} else {
		service = service.TrustFingerprints(true) // Default to true
	}

	resp, err := service.Do(ctx)
	return resp, WrapFivetranError(resp, err)
}
