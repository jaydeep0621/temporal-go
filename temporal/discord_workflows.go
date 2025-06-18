package temporal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type DiscordWorkflowManager struct {
	workflowClient *WorkflowClient
	logger         *logrus.Logger
	runningWorkflows map[string]chan struct{}
	mu             sync.RWMutex
}

func NewDiscordWorkflowManager(wc *WorkflowClient, logger *logrus.Logger) *DiscordWorkflowManager {
	return &DiscordWorkflowManager{
		workflowClient:   wc,
		logger:           logger,
		runningWorkflows: make(map[string]chan struct{}),
	}
}

func (dwm *DiscordWorkflowManager) RunDiscordMemberUpdateWorkflow(ctx context.Context, guildID, discordUserID string, roles []string, username string) error {
	workflowID := fmt.Sprintf("w-discordMember-%s-%s", guildID, uuid.New().String())

	_, err := dwm.workflowClient.StartWorkflow(ctx, workflowID, "discord-queue", DiscordMemberUpdateWorkflow, guildID, discordUserID, roles, username)
	if err != nil {
		dwm.logger.Errorf("Failed to start Discord member update workflow: %v", err)
		return err
	}

	dwm.logger.Infof("Started Discord member update workflow: %s", workflowID)
	return nil
}

func (dwm *DiscordWorkflowManager) RunDiscordChannelUpdateWorkflow(ctx context.Context, guildID, channelID, name string, channelType int) error {
	workflowID := fmt.Sprintf("w-discordChannel-Update-%s-%s", guildID, uuid.New().String())

	_, err := dwm.workflowClient.StartWorkflow(ctx, workflowID, "discord-queue", DiscordChannelUpdateWorkflow, guildID, channelID, name, channelType)
	if err != nil {
		dwm.logger.Errorf("Failed to start Discord channel update workflow: %v", err)
		return err
	}

	dwm.logger.Infof("Started Discord channel update workflow: %s", workflowID)
	return nil
}

func (dwm *DiscordWorkflowManager) RunDiscordGuildRoleUpdateWorkflow(ctx context.Context, guildID, roleID, name string) error {
	workflowID := fmt.Sprintf("w-discordGuildRole-Update-%s-%s", guildID, uuid.New().String())

	_, err := dwm.workflowClient.StartWorkflow(ctx, workflowID, "discord-queue", DiscordGuildRoleUpdateWorkflow, guildID, roleID, name)
	if err != nil {
		dwm.logger.Errorf("Failed to start Discord guild role update workflow: %v", err)
		return err
	}

	dwm.logger.Infof("Started Discord guild role update workflow: %s", workflowID)
	return nil
}

func (dwm *DiscordWorkflowManager) RunDiscordChannelMessageWorkflow(ctx context.Context, customerID, messageID, discordUserID string) error {
	queueKey := fmt.Sprintf("%s-%s", customerID, discordUserID)
	workflowID := fmt.Sprintf("discord-message-channel-%s", queueKey)

	return dwm.runSequentialWorkflow(ctx, queueKey, workflowID, "discord-queue", DiscordChannelMessageWorkflow, customerID, messageID)
}

func (dwm *DiscordWorkflowManager) RunDiscordMessageStreakWorkflow(ctx context.Context, messageID, customerID, discordUserID string) error {
	queueKey := fmt.Sprintf("%s-%s", customerID, discordUserID)
	workflowID := fmt.Sprintf("discord-message-streak-%s", queueKey)

	return dwm.runSequentialWorkflow(ctx, queueKey, workflowID, "discord-queue", DiscordMessageStreakWorkflow, messageID, customerID)
}

func (dwm *DiscordWorkflowManager) RunDiscordAnyMessageWorkflow(ctx context.Context, customerID, messageID, discordUserID string) error {
	queueKey := fmt.Sprintf("%s-%s", customerID, discordUserID)
	workflowID := fmt.Sprintf("discord-message-all-channel-%s", queueKey)

	return dwm.runSequentialWorkflow(ctx, queueKey, workflowID, "discord-queue", DiscordAnyMessageWorkflow, customerID, messageID)
}

func (dwm *DiscordWorkflowManager) RunDiscordFirstMessageWorkflow(ctx context.Context, customerID, messageID, discordUserID string) error {
	queueKey := fmt.Sprintf("%s-%s", customerID, discordUserID)
	workflowID := fmt.Sprintf("discord-first-message-%s", queueKey)

	return dwm.runSequentialWorkflow(ctx, queueKey, workflowID, "discord-queue", DiscordFirstMessageWorkflow, messageID, customerID)
}

func (dwm *DiscordWorkflowManager) RunDiscordRoleRewardWorkflow(ctx context.Context, customerID, roleID, discordUserID string) error {
	queueKey := fmt.Sprintf("%s-%s", customerID, discordUserID)
	workflowID := fmt.Sprintf("discord-role-%s", queueKey)

	return dwm.runSequentialWorkflow(ctx, queueKey, workflowID, "discord-queue", DiscordRoleRewardWorkflow, roleID, customerID)
}

func (dwm *DiscordWorkflowManager) runSequentialWorkflow(ctx context.Context, queueKey, workflowID, taskQueue string, workflowFunc interface{}, args ...interface{}) error {
	dwm.mu.Lock()
	previousWorkflow, exists := dwm.runningWorkflows[queueKey]
	if !exists {
		dwm.runningWorkflows[queueKey] = make(chan struct{})
		close(dwm.runningWorkflows[queueKey])
	}
	dwm.mu.Unlock()

	select {
	case <-previousWorkflow:
	case <-ctx.Done():
		return ctx.Err()
	}

	newWorkflow := make(chan struct{})
	dwm.mu.Lock()
	dwm.runningWorkflows[queueKey] = newWorkflow
	dwm.mu.Unlock()

	defer func() {
		dwm.mu.Lock()
		if dwm.runningWorkflows[queueKey] == newWorkflow {
			delete(dwm.runningWorkflows, queueKey)
		}
		dwm.mu.Unlock()
		close(newWorkflow)
	}()

	existingWorkflow := dwm.workflowClient.GetWorkflow(ctx, workflowID)
	if existingWorkflow != nil {
		err := existingWorkflow.Get(ctx, nil)
		if err != nil {
			dwm.logger.Infof("Previous workflow %s completed with error: %v", workflowID, err)
		} else {
			dwm.logger.Infof("Previous workflow %s completed successfully", workflowID)
		}
	}

	_, err := dwm.workflowClient.StartWorkflow(ctx, workflowID, taskQueue, workflowFunc, args...)
	if err != nil {
		dwm.logger.Errorf("Failed to start workflow %s: %v", workflowID, err)
		return err
	}

	dwm.logger.Infof("Started workflow %s", workflowID)
	return nil
}


func DiscordMemberUpdateWorkflow(ctx workflow.Context, guildID, discordUserID string, roles []string, username string) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Discord member update workflow",
		"guildID", guildID,
		"discordUserID", discordUserID,
		"roles", roles,
		"username", username)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	err := workflow.ExecuteActivity(ctx, "UpdateDiscordMember", guildID, discordUserID, roles, username).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to update Discord member", "error", err)
		return err
	}

	logger.Info("Discord member update workflow completed successfully")
	return nil
}

func DiscordChannelUpdateWorkflow(ctx workflow.Context, guildID, channelID, name string, channelType int) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Discord channel update workflow",
		"guildID", guildID,
		"channelID", channelID,
		"name", name,
		"type", channelType)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	err := workflow.ExecuteActivity(ctx, "UpdateDiscordChannel", guildID, channelID, name, channelType).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to update Discord channel", "error", err)
		return err
	}

	logger.Info("Discord channel update workflow completed successfully")
	return nil
}

func DiscordGuildRoleUpdateWorkflow(ctx workflow.Context, guildID, roleID, name string) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Discord guild role update workflow",
		"guildID", guildID,
		"roleID", roleID,
		"name", name)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	err := workflow.ExecuteActivity(ctx, "UpdateDiscordGuildRole", guildID, roleID, name).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to update Discord guild role", "error", err)
		return err
	}

	logger.Info("Discord guild role update workflow completed successfully")
	return nil
}

func DiscordChannelMessageWorkflow(ctx workflow.Context, customerID, messageID string) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Discord channel message workflow",
		"customerID", customerID,
		"messageID", messageID)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	err := workflow.ExecuteActivity(ctx, "ProcessChannelMessage", customerID, messageID).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to process channel message", "error", err)
		return err
	}

	logger.Info("Discord channel message workflow completed successfully")
	return nil
}

func DiscordMessageStreakWorkflow(ctx workflow.Context, messageID, customerID string) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Discord message streak workflow",
		"messageID", messageID,
		"customerID", customerID)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	err := workflow.ExecuteActivity(ctx, "ProcessMessageStreak", messageID, customerID).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to process message streak", "error", err)
		return err
	}

	logger.Info("Discord message streak workflow completed successfully")
	return nil
}

func DiscordAnyMessageWorkflow(ctx workflow.Context, customerID, messageID string) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Discord any message workflow",
		"customerID", customerID,
		"messageID", messageID)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	err := workflow.ExecuteActivity(ctx, "ProcessAnyMessage", customerID, messageID).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to process any message", "error", err)
		return err
	}

	logger.Info("Discord any message workflow completed successfully")
	return nil
}

func DiscordFirstMessageWorkflow(ctx workflow.Context, messageID, customerID string) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Discord first message workflow",
		"messageID", messageID,
		"customerID", customerID)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	err := workflow.ExecuteActivity(ctx, "ProcessFirstMessage", messageID, customerID).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to process first message", "error", err)
		return err
	}

	logger.Info("Discord first message workflow completed successfully")
	return nil
}

func DiscordRoleRewardWorkflow(ctx workflow.Context, roleID, customerID string) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Discord role reward workflow",
		"roleID", roleID,
		"customerID", customerID)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})

	err := workflow.ExecuteActivity(ctx, "ProcessRoleReward", roleID, customerID).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to process role reward", "error", err)
		return err
	}

	logger.Info("Discord role reward workflow completed successfully")
	return nil
}