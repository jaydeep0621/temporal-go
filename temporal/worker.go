package temporal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type WorkerManager struct {
	temporalManager *TemporalManager
	logger          *logrus.Logger
	workers         map[string]worker.Worker
	mu              sync.RWMutex
}

func NewWorkerManager(tm *TemporalManager, logger *logrus.Logger) *WorkerManager {
	return &WorkerManager{
		temporalManager: tm,
		logger:          logger,
		workers:         make(map[string]worker.Worker),
	}
}

func (wm *WorkerManager) StartWorker(taskQueue string, activities []interface{}) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if _, exists := wm.workers[taskQueue]; exists {
		return fmt.Errorf("worker for task queue %s already exists", taskQueue)
	}

	workerOptions := worker.Options{
		MaxConcurrentActivityExecutionSize: 10,
		MaxConcurrentWorkflowTaskExecutionSize: 10,
		MaxConcurrentLocalActivityExecutionSize: 10,
	}

	w, err := wm.temporalManager.CreateWorker(taskQueue, workerOptions)
	if err != nil {
		return fmt.Errorf("failed to create worker: %w", err)
	}

	for _, activity := range activities {
		w.RegisterActivity(activity)
	}

	w.RegisterWorkflow(DiscordWorkflow)

	if err := w.Start(); err != nil {
		return fmt.Errorf("failed to start worker: %w", err)
	}

	wm.workers[taskQueue] = w
	wm.logger.Infof("Started worker for task queue: %s", taskQueue)

	return nil
}

func (wm *WorkerManager) StopWorker(taskQueue string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	w, exists := wm.workers[taskQueue]
	if !exists {
		return fmt.Errorf("worker for task queue %s not found", taskQueue)
	}

	w.Stop()
	delete(wm.workers, taskQueue)
	wm.logger.Infof("Stopped worker for task queue: %s", taskQueue)

	return nil
}

func (wm *WorkerManager) StopAllWorkers() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for taskQueue, w := range wm.workers {
		w.Stop()
		wm.logger.Infof("Stopped worker for task queue: %s", taskQueue)
	}

	wm.workers = make(map[string]worker.Worker)
}

func DiscordWorkflow(ctx workflow.Context, input DiscordWorkflowInput) (DiscordWorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Discord workflow", "input", input)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	var result DiscordWorkflowOutput

	err := workflow.ExecuteActivity(ctx, "SendDiscordMessage", input.Message).Get(ctx, &result.Message)
	if err != nil {
		logger.Error("Failed to send Discord message", "error", err)
		return result, err
	}

	logger.Info("Discord workflow completed successfully")
	return result, nil
}

func CampaignWorkflow(ctx workflow.Context, input CampaignWorkflowInput) (CampaignWorkflowOutput, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting campaign workflow", "input", input)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    5,
		},
	})

	var result CampaignWorkflowOutput

	err := workflow.ExecuteActivity(ctx, "CreateCampaign", input.CampaignData).Get(ctx, &result.CampaignID)
	if err != nil {
		logger.Error("Failed to create campaign", "error", err)
		return result, err
	}

	err = workflow.ExecuteActivity(ctx, "WaitForCampaignReady", result.CampaignID).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to wait for campaign ready", "error", err)
		return result, err
	}

	logger.Info("Campaign workflow completed successfully")
	return result, nil
}

type DiscordWorkflowInput struct {
	Message   string `json:"message"`
	ChannelID string `json:"channelId"`
	UserID    string `json:"userId"`
}

type DiscordWorkflowOutput struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type CampaignWorkflowInput struct {
	CampaignData map[string]interface{} `json:"campaignData"`
	CustomerID   string                 `json:"customerId"`
}

type CampaignWorkflowOutput struct {
	CampaignID string `json:"campaignId"`
	Status     string `json:"status"`
}

type DiscordActivities struct {
	logger *logrus.Logger
}

func NewDiscordActivities(logger *logrus.Logger) *DiscordActivities {
	return &DiscordActivities{
		logger: logger,
	}
}

func (da *DiscordActivities) SendDiscordMessage(ctx context.Context, message string) (string, error) {
	da.logger.Infof("Sending Discord message: %s", message)

	time.Sleep(2 * time.Second)

	return fmt.Sprintf("Message sent: %s", message), nil
}

type CampaignActivities struct {
	logger *logrus.Logger
}

func NewCampaignActivities(logger *logrus.Logger) *CampaignActivities {
	return &CampaignActivities{
		logger: logger,
	}
}

func (ca *CampaignActivities) CreateCampaign(ctx context.Context, campaignData map[string]interface{}) (string, error) {
	ca.logger.Info("Creating campaign", "data", campaignData)

	time.Sleep(5 * time.Second)

	campaignID := fmt.Sprintf("campaign_%d", time.Now().Unix())
	return campaignID, nil
}

func (ca *CampaignActivities) WaitForCampaignReady(ctx context.Context, campaignID string) error {
	ca.logger.Infof("Waiting for campaign %s to be ready", campaignID)

	time.Sleep(3 * time.Second)

	return nil
}