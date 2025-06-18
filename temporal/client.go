package temporal

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type TemporalManager struct {
	client    client.Client
	namespace string
	logger    *logrus.Logger
	mu        sync.RWMutex
}

type ConnectionOptions struct {
	Address string
	TLS     *tls.Config
}

var (
	temporalClient *TemporalManager
	once           sync.Once
)

func GetTemporalClient(logger *logrus.Logger) (*TemporalManager, error) {
	var err error
	once.Do(func() {
		temporalClient, err = newTemporalManager(logger)
	})
	return temporalClient, err
}

func newTemporalManager(logger *logrus.Logger) (*TemporalManager, error) {
	connectionOptions, err := getConnectionOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to get connection options: %w", err)
	}

	namespace := getNamespace()

	clientOptions := client.Options{
		Namespace: namespace,
	}

	if connectionOptions.Address != "" {
		clientOptions.HostPort = connectionOptions.Address
	}

	if connectionOptions.TLS != nil {
		clientOptions.ConnectionOptions = client.ConnectionOptions{
			TLS: connectionOptions.TLS,
		}
	}

	c, err := client.Dial(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create Temporal client: %w", err)
	}

	logger.Infof("Temporal client connected to %s with namespace: %s",
		connectionOptions.Address, namespace)

	return &TemporalManager{
		client:    c,
		namespace: namespace,
		logger:    logger,
	}, nil
}

func (tm *TemporalManager) GetClient() client.Client {
	return tm.client
}

func (tm *TemporalManager) GetNamespace() string {
	return tm.namespace
}

func (tm *TemporalManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.client != nil {
		tm.client.Close()
		tm.logger.Info("Temporal client connection closed")
	}
	return nil
}

func (tm *TemporalManager) CreateWorker(taskQueue string, options worker.Options) (worker.Worker, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if tm.client == nil {
		return nil, fmt.Errorf("Temporal client not initialized")
	}

	w := worker.New(tm.client, taskQueue, options)
	tm.logger.Infof("Created Temporal worker for task queue: %s", taskQueue)

	return w, nil
}

func getConnectionOptions() (*ConnectionOptions, error) {
	nodeEnv := os.Getenv("NODE_ENV")
	if nodeEnv == "" {
		nodeEnv = "development"
	}

	isDeployed := nodeEnv == "production" || nodeEnv == "staging"

	if !isDeployed {
		return &ConnectionOptions{
			Address: "localhost:7233",
			TLS:     nil,
		}, nil
	}

	certPath := os.Getenv("TEMPORAL_CERT_PATH")
	keyPath := os.Getenv("TEMPORAL_CERT_KEY_PATH")
	namespace := os.Getenv("TEMPORAL_NAMESPACE")

	if certPath == "" || keyPath == "" || namespace == "" {
		return nil, fmt.Errorf(
			"TEMPORAL_CERT_PATH, TEMPORAL_CERT_KEY_PATH and TEMPORAL_NAMESPACE are required when NODE_ENV=%s, but not found in the environment",
			nodeEnv,
		)
	}

	cert, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate file: %w", err)
	}

	key, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	tlsCert, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	}

	return &ConnectionOptions{
		Address: fmt.Sprintf("%s.tmprl.cloud:7233", namespace),
		TLS:     tlsConfig,
	}, nil
}

func getNamespace() string {
	nodeEnv := os.Getenv("NODE_ENV")
	if nodeEnv == "development" {
		return "default"
	}
	return os.Getenv("TEMPORAL_NAMESPACE")
}

type WorkflowClient struct {
	client client.Client
	logger *logrus.Logger
}

func NewWorkflowClient(tm *TemporalManager) *WorkflowClient {
	return &WorkflowClient{
		client: tm.GetClient(),
		logger: tm.logger,
	}
}

func (wc *WorkflowClient) StartWorkflow(ctx context.Context, workflowID, taskQueue string, workflowType interface{}, args ...interface{}) (client.WorkflowRun, error) {
	options := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}

	run, err := wc.client.ExecuteWorkflow(ctx, options, workflowType, args...)
	if err != nil {
		wc.logger.Errorf("Failed to start workflow %s: %v", workflowID, err)
		return nil, err
	}

	wc.logger.Infof("Started workflow %s with run ID: %s", workflowID, run.GetRunID())
	return run, nil
}

func (wc *WorkflowClient) GetWorkflow(ctx context.Context, workflowID string) client.WorkflowRun {
	return wc.client.GetWorkflow(ctx, workflowID, "")
}

func (wc *WorkflowClient) CancelWorkflow(ctx context.Context, workflowID string) error {
	err := wc.client.CancelWorkflow(ctx, workflowID, "")
	if err != nil {
		wc.logger.Errorf("Failed to cancel workflow %s: %v", workflowID, err)
		return err
	}

	wc.logger.Infof("Cancelled workflow %s", workflowID)
	return nil
}

func (wc *WorkflowClient) TerminateWorkflow(ctx context.Context, workflowID, reason string) error {
	err := wc.client.TerminateWorkflow(ctx, workflowID, "", reason)
	if err != nil {
		wc.logger.Errorf("Failed to terminate workflow %s: %v", workflowID, err)
		return err
	}

	wc.logger.Infof("Terminated workflow %s with reason: %s", workflowID, reason)
	return nil
}