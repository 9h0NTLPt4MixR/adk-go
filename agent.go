// Copyright 2025 The adk-go Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package adk provides the core Agent Development Kit for building AI agents.
package adk

import (
	"context"
	"errors"
)

// Agent represents an AI agent capable of processing messages and invoking tools.
type Agent interface {
	// Name returns the unique name of the agent.
	Name() string

	// Description returns a human-readable description of the agent's capabilities.
	Description() string

	// Run executes the agent with the given input and returns the result.
	Run(ctx context.Context, input *RunInput) (*RunOutput, error)
}

// RunInput holds the input data for an agent run.
type RunInput struct {
	// Messages contains the conversation history and the current user message.
	Messages []*Message

	// SessionID is an optional identifier for maintaining conversation state.
	SessionID string
}

// RunOutput holds the result of an agent run.
type RunOutput struct {
	// Messages contains the agent's response messages.
	Messages []*Message

	// SessionID echoes back the session identifier from the input.
	SessionID string
}

// Message represents a single message in a conversation.
type Message struct {
	// Role identifies who sent the message (e.g., "user", "assistant", "system").
	Role Role

	// Content holds the text content of the message.
	Content string

	// ToolCalls contains any tool invocations requested by the model.
	ToolCalls []*ToolCall

	// ToolResults contains results from tool invocations.
	ToolResults []*ToolResult
}

// Role represents the sender role of a message.
type Role string

const (
	// RoleUser represents a message from the human user.
	RoleUser Role = "user"

	// RoleAssistant represents a message from the AI assistant.
	RoleAssistant Role = "assistant"

	// RoleSystem represents a system-level instruction message.
	RoleSystem Role = "system"

	// RoleTool represents a message containing tool execution results.
	RoleTool Role = "tool"
)

// ToolCall represents a request from the model to invoke a tool.
type ToolCall struct {
	// ID is a unique identifier for this tool call.
	ID string

	// Name is the name of the tool to invoke.
	Name string

	// Arguments contains the JSON-encoded arguments for the tool.
	Arguments string
}

// ToolResult holds the result of a tool invocation.
type ToolResult struct {
	// ToolCallID references the ToolCall this result corresponds to.
	ToolCallID string

	// Content contains the tool's output.
	Content string

	// Error contains any error message if the tool invocation failed.
	Error string
}

// ErrAgentNotFound is returned when a requested agent cannot be found.
var ErrAgentNotFound = errors.New("agent not found")

// ErrInvalidInput is returned when the provided run input is invalid.
var ErrInvalidInput = errors.New("invalid input")
