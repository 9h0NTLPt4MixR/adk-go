// Copyright 2026 Google LLC
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

package workflowagent

import (
	"encoding/json"
	"iter"

	"google.golang.org/genai"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/session"
	"google.golang.org/adk/workflow"
)

// Config is the configuration for creating a new Workflow agent.
type Config struct {
	Name                 string
	Description          string
	SubAgents            []agent.Agent
	BeforeAgentCallbacks []agent.BeforeAgentCallback
	AfterAgentCallbacks  []agent.AfterAgentCallback
	Edges                []workflow.Edge
}

// New creates a new Workflow agent. The returned agent.Agent is
// stateless and ready to plug into a runner.Runner; per-invocation
// run state is persisted in session.State so a paused workflow can
// be resumed on a follow-up turn by submitting a FunctionResponse
// targeting the InterruptID emitted by the paused node.
func New(cfg Config) (agent.Agent, error) {
	w, err := workflow.New(cfg.Name, cfg.Edges)
	if err != nil {
		return nil, err
	}

	wa := &workflowAgent{workflow: w}

	return agent.New(agent.Config{
		Name:                 cfg.Name,
		Description:          cfg.Description,
		SubAgents:            cfg.SubAgents,
		BeforeAgentCallbacks: cfg.BeforeAgentCallbacks,
		AfterAgentCallbacks:  cfg.AfterAgentCallbacks,
		Run:                  wa.run,
	})
}

// workflowAgent is the stateless wrapper that dispatches between
// Workflow.Run (fresh turn) and Workflow.Resume (resume turn). The
// dispatch decision is made by inspecting ctx.UserContent for a
// FunctionResponse targeting a previously-emitted RequestInput;
// the workflow's RunState lives in session.State, not on this
// struct, so a single *workflowAgent safely services many
// concurrent sessions.
type workflowAgent struct {
	workflow *workflow.Workflow
}

// run is the agent.Config.Run callback. The yield-and-return loop
// is the standard adk-go pattern for forwarding events from one
// iter.Seq2 through another while honouring a downstream caller's
// break.
func (a *workflowAgent) run(ctx agent.InvocationContext) iter.Seq2[*session.Event, error] {
	return func(yield func(*session.Event, error) bool) {
		if responses, state, ok := a.detectResume(ctx); ok {
			for ev, err := range a.workflow.Resume(ctx, state, responses) {
				if !yield(ev, err) {
					return
				}
			}
			return
		}
		for ev, err := range a.workflow.Run(ctx) {
			if !yield(ev, err) {
				return
			}
		}
	}
}

// detectResume inspects the inbound user message for FunctionResponses
// targeting a previously-emitted RequestInput. Returns the
// responses map keyed by InterruptID (suitable for
// Workflow.Resume), the RunState loaded from session, and true if
// this turn is a resume; (nil, nil, false) for a fresh turn.
//
// The runner has already verified by FunctionCall.ID lookup that
// this turn belongs to this agent. Here we only need to recognise
// the magic name and decode the payload, analogous to the way
// RequestConfirmationRequestProcessor handles tool confirmation
// inside the LLMAgent flow.
func (a *workflowAgent) detectResume(ctx agent.InvocationContext) (map[string]any, *workflow.RunState, bool) {
	frs := functionResponsesIn(ctx.UserContent())
	if len(frs) == 0 {
		return nil, nil, false
	}

	responses := map[string]any{}
	for _, fr := range frs {
		if fr.Name != workflow.WorkflowInputFunctionCallName {
			continue
		}
		responses[fr.ID] = decodeWorkflowInputResponse(fr)
	}
	if len(responses) == 0 {
		return nil, nil, false
	}

	state, err := workflow.LoadRunState(ctx.Session(), a.workflow.Name())
	if err != nil || state == nil {
		// No persisted state means there is nothing to resume;
		// fall through to a fresh Workflow.Run.
		return nil, nil, false
	}

	return responses, state, true
}

// functionResponsesIn extracts FunctionResponse parts from a user
// content message. Local re-implementation of internal/utils.
// FunctionResponses to avoid taking a dependency on internal/.
func functionResponsesIn(content *genai.Content) []*genai.FunctionResponse {
	if content == nil {
		return nil
	}
	var out []*genai.FunctionResponse
	for _, p := range content.Parts {
		if p == nil || p.FunctionResponse == nil {
			continue
		}
		out = append(out, p.FunctionResponse)
	}
	return out
}

// decodeWorkflowInputResponse extracts the user-supplied payload
// from a FunctionResponse targeting a workflow input request.
// Mirrors the dual-format handling used by tool confirmation: ADK
// web wraps the payload as {"response": "<json string>"}, while
// other clients inline {"payload": ...} directly. Falls back to
// the whole Response map if neither shape matches.
func decodeWorkflowInputResponse(fr *genai.FunctionResponse) any {
	if fr == nil {
		return nil
	}
	if raw, ok := fr.Response["response"]; ok {
		if s, isStr := raw.(string); isStr {
			var decoded any
			if err := json.Unmarshal([]byte(s), &decoded); err == nil {
				return decoded
			}
			return s
		}
		return raw
	}
	if payload, ok := fr.Response["payload"]; ok {
		return payload
	}
	return fr.Response
}
