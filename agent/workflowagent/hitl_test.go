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
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"google.golang.org/genai"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/session"
	"google.golang.org/adk/workflow"
)

// statefulSession is a minimal session.Session implementation that
// faithfully models the AppendEvent-gated state contract used by
// session.InMemoryService and persistent backends: state mutations
// are applied only when applyStateDelta is called for an event
// (the unit-test analogue of session.Service.AppendEvent), never
// via direct State.Set from inside the agent. This mirrors the
// guidance in https://adk.dev/sessions/state/#a-warning-about-direct-state-modification
// and the planned removal of in-invocation State.Set tracked in
// b/492152475.
//
// drainAgent (this file) calls applyStateDelta for every event the
// agent yields, simulating what the runner does in production.
type statefulSession struct {
	session.Session
	state *statefulState
}

func newStatefulSession() *statefulSession {
	return &statefulSession{state: &statefulState{m: map[string]any{}}}
}

func (s *statefulSession) ID() string           { return "test-session-id" }
func (s *statefulSession) State() session.State { return s.state }

// applyStateDelta merges any Actions.StateDelta on the supplied
// event into the underlying state map. Mirrors what
// inMemoryService.AppendEvent does for session-scoped (no
// app:/user:/temp: prefix) keys; HITL persistence uses such keys.
func (s *statefulSession) applyStateDelta(ev *session.Event) {
	if ev == nil || len(ev.Actions.StateDelta) == 0 {
		return
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	for k, v := range ev.Actions.StateDelta {
		s.state.m[k] = v
	}
}

// statefulState exposes session.State semantics with one subtle
// constraint compared to the real services: callers that bypass
// the runner cannot mutate state via Set; they must construct an
// event with Actions.StateDelta and route it through
// statefulSession.applyStateDelta instead. Get reflects the
// AppendEvent-applied view.
type statefulState struct {
	mu sync.Mutex
	m  map[string]any
}

func (s *statefulState) Get(key string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	if !ok {
		return nil, session.ErrStateKeyNotExist
	}
	return v, nil
}

// Set is a no-op writer in this mock to surface accidental direct
// modification (b/492152475). Production code must route state
// changes through Event.Actions.StateDelta. Tests that need to
// pre-seed state can write directly to the underlying map via the
// statefulSession constructor.
func (s *statefulState) Set(key string, value any) error {
	// Intentionally not persisted: real session services do not
	// propagate direct Set from inside an invocation either.
	// Returning nil keeps the call non-fatal so production code
	// that defensively writes through State.Set still compiles
	// and runs.
	return nil
}

func (s *statefulState) All() iter.Seq2[string, any] {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := make(map[string]any, len(s.m))
	for k, v := range s.m {
		snapshot[k] = v
	}
	return func(yield func(string, any) bool) {
		for k, v := range snapshot {
			if !yield(k, v) {
				return
			}
		}
	}
}

// hitlNode is a custom Node used by the HITL resume tests. The
// Run callback is supplied per test so each scenario can shape
// its own emission pattern.
type hitlNode struct {
	workflow.BaseNode
	run func(ctx agent.InvocationContext, input any, yield func(*session.Event, error) bool)
}

func newHitlNode(name string, run func(ctx agent.InvocationContext, input any, yield func(*session.Event, error) bool)) *hitlNode {
	return &hitlNode{
		BaseNode: workflow.NewBaseNode(name, "", workflow.NodeConfig{}),
		run:      run,
	}
}

func (n *hitlNode) Run(ctx agent.InvocationContext, input any) iter.Seq2[*session.Event, error] {
	return func(yield func(*session.Event, error) bool) {
		n.run(ctx, input, yield)
	}
}

// makeAgent builds a workflowagent with the given edges and the
// canonical "test_workflow" name (the name is what
// session.State persistence is keyed by).
func makeAgent(t *testing.T, edges []workflow.Edge) agent.Agent {
	t.Helper()
	a, err := New(Config{Name: "test_workflow", Edges: edges})
	if err != nil {
		t.Fatalf("workflowagent.New: %v", err)
	}
	return a
}

// runCtx returns an InvocationContext suitable for driving the
// workflow agent. The same session is reused across calls so
// pause/resume round-trips through statefulState as they would
// in production.
func runCtx(sess session.Session, agt agent.Agent, msg *genai.Content) *MockInvocationContext {
	return &MockInvocationContext{
		Context:     context.TODO(),
		sess:        sess,
		userContent: msg,
		myAgent:     agt,
	}
}

// drainAgent consumes the agent's iter.Seq2, collecting events,
// and applies each event's StateDelta to sess. The applyStateDelta
// step replaces the AppendEvent-side state propagation that the
// real runner performs; without it state writes from the agent
// would never become visible to subsequent calls. Fails the test
// if the iterator yields a non-nil error that the test did not
// opt into via wantErr.
func drainAgent(t *testing.T, sess *statefulSession, seq iter.Seq2[*session.Event, error], wantErr error) []*session.Event {
	t.Helper()
	var got []*session.Event
	var sawErr error
	for ev, err := range seq {
		if err != nil {
			if sawErr == nil {
				sawErr = err
			}
			continue
		}
		got = append(got, ev)
		sess.applyStateDelta(ev)
	}
	switch {
	case wantErr == nil && sawErr != nil:
		t.Fatalf("unexpected error from agent: %v", sawErr)
	case wantErr != nil && sawErr == nil:
		t.Fatalf("expected error %v, got none", wantErr)
	case wantErr != nil && !errors.Is(sawErr, wantErr):
		t.Fatalf("expected error %v, got %v", wantErr, sawErr)
	}
	return got
}

// findRequest scans events for the first one carrying a
// RequestedInput and returns the InterruptID it carried, or "" if
// none was found.
func findRequest(events []*session.Event) string {
	for _, ev := range events {
		if ev != nil && ev.RequestedInput != nil {
			return ev.RequestedInput.InterruptID
		}
	}
	return ""
}

// resumeMessage builds a user message carrying a FunctionResponse
// that targets a previously-emitted RequestInput.
func resumeMessage(interruptID string, payload any) *genai.Content {
	return &genai.Content{
		Parts: []*genai.Part{{
			FunctionResponse: &genai.FunctionResponse{
				ID:   interruptID,
				Name: workflow.WorkflowInputFunctionCallName,
				Response: map[string]any{
					"payload": payload,
				},
			},
		}},
	}
}

// TestWorkflowAgent_RunThenResume_Handoff exercises the canonical
// round-trip: a fresh Run pauses on a node that requested input,
// and a follow-up Resume turn delivers the response which flows
// to the asker's successor as its input.
func TestWorkflowAgent_RunThenResume_Handoff(t *testing.T) {
	var handlerInput atomic.Value // captured by the handler

	asker := newHitlNode("asker", func(ctx agent.InvocationContext, _ any, yield func(*session.Event, error) bool) {
		yield(workflow.NewRequestInputEvent(ctx, session.RequestInput{
			InterruptID: "approve_or_reject",
			Message:     "Please decide",
		}), nil)
	})
	handler := workflow.NewFunctionNode(
		"handler",
		func(ctx agent.InvocationContext, input string) (string, error) {
			handlerInput.Store(input)
			return "handled:" + input, nil
		},
		workflow.NodeConfig{},
	)

	a := makeAgent(t, workflow.Chain(workflow.Start, asker, handler))
	sess := newStatefulSession()

	// Turn 1: fresh Run; should pause with a RequestedInput.
	turn1 := drainAgent(t, sess, a.Run(runCtx(sess, a, &genai.Content{
		Parts: []*genai.Part{{Text: "draft"}},
	})), nil)
	if got := findRequest(turn1); got != "approve_or_reject" {
		t.Fatalf("turn 1 RequestedInput = %q, want %q", got, "approve_or_reject")
	}
	if v := handlerInput.Load(); v != nil {
		t.Errorf("handler ran during turn 1; got input %v, want it not to run", v)
	}

	// Turn 2: resume with a payload; handler should run and
	// receive the payload as its input.
	turn2 := drainAgent(t, sess, a.Run(runCtx(sess, a, resumeMessage("approve_or_reject", "approve"))), nil)
	if findRequest(turn2) != "" {
		t.Errorf("turn 2 unexpectedly emitted a RequestedInput")
	}
	if got, want := handlerInput.Load(), "approve"; got != want {
		t.Errorf("handler input = %v, want %q", got, want)
	}
}

// TestWorkflowAgent_Resume_RestoresStateFromSession verifies that
// the run state survives between agent instances backed by the
// same session: after Run, a fresh agent built from the same
// edges (simulating a process restart) can still Resume.
func TestWorkflowAgent_Resume_RestoresStateFromSession(t *testing.T) {
	var handlerCalled atomic.Bool

	makeNodes := func() (workflow.Node, workflow.Node) {
		asker := newHitlNode("asker", func(ctx agent.InvocationContext, _ any, yield func(*session.Event, error) bool) {
			yield(workflow.NewRequestInputEvent(ctx, session.RequestInput{
				InterruptID: "human_approval",
				Message:     "approve?",
			}), nil)
		})
		handler := workflow.NewFunctionNode(
			"handler",
			func(ctx agent.InvocationContext, input string) (any, error) {
				handlerCalled.Store(true)
				return nil, nil
			},
			workflow.NodeConfig{},
		)
		return asker, handler
	}

	sess := newStatefulSession()

	// First agent instance: Run → pause.
	asker1, handler1 := makeNodes()
	a1 := makeAgent(t, workflow.Chain(workflow.Start, asker1, handler1))
	turn1 := drainAgent(t, sess, a1.Run(runCtx(sess, a1, &genai.Content{Parts: []*genai.Part{{Text: "draft"}}})), nil)
	if findRequest(turn1) != "human_approval" {
		t.Fatalf("first agent did not pause as expected")
	}

	// Second agent instance, same session: Resume.
	asker2, handler2 := makeNodes()
	a2 := makeAgent(t, workflow.Chain(workflow.Start, asker2, handler2))
	drainAgent(t, sess, a2.Run(runCtx(sess, a2, resumeMessage("human_approval", "yes"))), nil)
	if !handlerCalled.Load() {
		t.Error("handler did not run after resume on a fresh agent instance")
	}
}

// TestWorkflowAgent_Resume_Idempotent verifies that two Resume
// calls with the same payload run the handler only once.
func TestWorkflowAgent_Resume_Idempotent(t *testing.T) {
	var handlerRuns atomic.Int32

	asker := newHitlNode("asker", func(ctx agent.InvocationContext, _ any, yield func(*session.Event, error) bool) {
		yield(workflow.NewRequestInputEvent(ctx, session.RequestInput{
			InterruptID: "approve",
			Message:     "?",
		}), nil)
	})
	handler := workflow.NewFunctionNode(
		"handler",
		func(ctx agent.InvocationContext, input string) (any, error) {
			handlerRuns.Add(1)
			return nil, nil
		},
		workflow.NodeConfig{},
	)

	a := makeAgent(t, workflow.Chain(workflow.Start, asker, handler))
	sess := newStatefulSession()

	drainAgent(t, sess, a.Run(runCtx(sess, a, &genai.Content{Parts: []*genai.Part{{Text: "x"}}})), nil)
	drainAgent(t, sess, a.Run(runCtx(sess, a, resumeMessage("approve", "yes"))), nil)
	drainAgent(t, sess, a.Run(runCtx(sess, a, resumeMessage("approve", "yes"))), nil)

	if got := handlerRuns.Load(); got != 1 {
		t.Errorf("handler runs = %d, want 1 (duplicate Resume must be a no-op)", got)
	}
}

// TestWorkflowAgent_Resume_NoMatchingResponse exercises the empty-
// responses guard: a Resume turn that carries a FunctionResponse
// for an InterruptID that no longer matches a waiting node falls
// through to a fresh Run rather than blocking on an empty
// scheduler.
func TestWorkflowAgent_Resume_NoMatchingResponse(t *testing.T) {
	asker := newHitlNode("asker", func(ctx agent.InvocationContext, _ any, yield func(*session.Event, error) bool) {
		yield(workflow.NewRequestInputEvent(ctx, session.RequestInput{
			InterruptID: "real_id",
			Message:     "?",
		}), nil)
	})

	a := makeAgent(t, workflow.Chain(workflow.Start, asker))
	sess := newStatefulSession()

	// Pause once.
	drainAgent(t, sess, a.Run(runCtx(sess, a, &genai.Content{Parts: []*genai.Part{{Text: "x"}}})), nil)

	// Submit a FunctionResponse for an unknown ID. detectResume
	// will see the magic name, load state, build a responses map,
	// but no waiting node will match — Resume returns immediately,
	// no events.
	turn := drainAgent(t, sess, a.Run(runCtx(sess, a, resumeMessage("unknown_id", "x"))), nil)
	if findRequest(turn) != "" {
		t.Errorf("unmatched resume produced a new RequestedInput; got %v", turn)
	}
}

// TestWorkflowAgent_Resume_SchemaValidation_Pass verifies that a
// response payload conforming to ResponseSchema is delivered to
// the handler unchanged (the validator coerces but here the
// shape already matches).
func TestWorkflowAgent_Resume_SchemaValidation_Pass(t *testing.T) {
	var handlerInput atomic.Value

	approvalSchema := &jsonschema.Schema{
		Type: "object",
		Properties: map[string]*jsonschema.Schema{
			"approved": {Type: "boolean"},
		},
		Required: []string{"approved"},
	}

	asker := newHitlNode("asker", func(ctx agent.InvocationContext, _ any, yield func(*session.Event, error) bool) {
		yield(workflow.NewRequestInputEvent(ctx, session.RequestInput{
			InterruptID:    "approval",
			Message:        "decide",
			ResponseSchema: approvalSchema,
		}), nil)
	})
	handler := workflow.NewFunctionNode(
		"handler",
		func(ctx agent.InvocationContext, input map[string]any) (any, error) {
			handlerInput.Store(input)
			return nil, nil
		},
		workflow.NodeConfig{},
	)

	a := makeAgent(t, workflow.Chain(workflow.Start, asker, handler))
	sess := newStatefulSession()

	drainAgent(t, sess, a.Run(runCtx(sess, a, &genai.Content{Parts: []*genai.Part{{Text: "x"}}})), nil)
	drainAgent(t, sess, a.Run(runCtx(sess, a, resumeMessage("approval", map[string]any{"approved": true}))), nil)

	got, ok := handlerInput.Load().(map[string]any)
	if !ok || got["approved"] != true {
		t.Errorf("handler input = %v, want map with approved=true", handlerInput.Load())
	}
}

// TestWorkflowAgent_Resume_SchemaValidation_Fail verifies that a
// response payload that violates ResponseSchema surfaces
// ErrInvalidResumeResponse and leaves the node parked, so a
// follow-up turn with a corrected payload still works.
func TestWorkflowAgent_Resume_SchemaValidation_Fail(t *testing.T) {
	var handlerRuns atomic.Int32

	approvalSchema := &jsonschema.Schema{
		Type: "object",
		Properties: map[string]*jsonschema.Schema{
			"approved": {Type: "boolean"},
		},
		Required: []string{"approved"},
	}

	asker := newHitlNode("asker", func(ctx agent.InvocationContext, _ any, yield func(*session.Event, error) bool) {
		yield(workflow.NewRequestInputEvent(ctx, session.RequestInput{
			InterruptID:    "approval",
			Message:        "decide",
			ResponseSchema: approvalSchema,
		}), nil)
	})
	handler := workflow.NewFunctionNode(
		"handler",
		func(ctx agent.InvocationContext, input map[string]any) (any, error) {
			handlerRuns.Add(1)
			return nil, nil
		},
		workflow.NodeConfig{},
	)

	a := makeAgent(t, workflow.Chain(workflow.Start, asker, handler))
	sess := newStatefulSession()

	// Pause.
	drainAgent(t, sess, a.Run(runCtx(sess, a, &genai.Content{Parts: []*genai.Part{{Text: "x"}}})), nil)

	// Submit invalid payload (string instead of {approved: bool}).
	drainAgent(t, sess, a.Run(runCtx(sess, a, resumeMessage("approval", "not an object"))), workflow.ErrInvalidResumeResponse)
	if handlerRuns.Load() != 0 {
		t.Fatal("handler ran despite schema validation failure")
	}

	// Retry with valid payload — should succeed.
	drainAgent(t, sess, a.Run(runCtx(sess, a, resumeMessage("approval", map[string]any{"approved": true}))), nil)
	if handlerRuns.Load() != 1 {
		t.Errorf("handler runs after retry = %d, want 1", handlerRuns.Load())
	}
}

// TestWorkflowAgent_Resume_FanOut verifies that a handoff resume
// from an asker with multiple successors fans out the response
// to every successor, exactly as a normal output would.
func TestWorkflowAgent_Resume_FanOut(t *testing.T) {
	var hits atomic.Int32

	asker := newHitlNode("asker", func(ctx agent.InvocationContext, _ any, yield func(*session.Event, error) bool) {
		yield(workflow.NewRequestInputEvent(ctx, session.RequestInput{
			InterruptID: "fan",
			Message:     "?",
		}), nil)
	})
	makeHandler := func(name string) workflow.Node {
		return workflow.NewFunctionNode(
			name,
			func(ctx agent.InvocationContext, input string) (any, error) {
				hits.Add(1)
				return nil, nil
			},
			workflow.NodeConfig{},
		)
	}
	h1 := makeHandler("h1")
	h2 := makeHandler("h2")
	h3 := makeHandler("h3")

	a := makeAgent(t, []workflow.Edge{
		{From: workflow.Start, To: asker},
		{From: asker, To: h1},
		{From: asker, To: h2},
		{From: asker, To: h3},
	})
	sess := newStatefulSession()

	drainAgent(t, sess, a.Run(runCtx(sess, a, &genai.Content{Parts: []*genai.Part{{Text: "x"}}})), nil)
	drainAgent(t, sess, a.Run(runCtx(sess, a, resumeMessage("fan", "go"))), nil)

	if got := hits.Load(); got != 3 {
		t.Errorf("successor hits = %d, want 3", got)
	}
}

// TestWorkflowAgent_FreshTurn_NotMistakenForResume verifies the
// detectResume guard: a fresh user message that happens to have
// no FunctionResponse part does NOT trip the resume path even if
// a RunState is persisted (e.g. from a completed prior workflow).
// Important because session.State may carry leftover state from
// previous runs.
func TestWorkflowAgent_FreshTurn_NotMistakenForResume(t *testing.T) {
	var firstRun atomic.Bool
	var secondRun atomic.Bool

	makeAsker := func(flag *atomic.Bool) workflow.Node {
		return newHitlNode("asker", func(ctx agent.InvocationContext, _ any, yield func(*session.Event, error) bool) {
			flag.Store(true)
			yield(workflow.NewRequestInputEvent(ctx, session.RequestInput{
				InterruptID: "ask",
				Message:     "?",
			}), nil)
		})
	}

	a := makeAgent(t, workflow.Chain(workflow.Start, makeAsker(&firstRun)))
	sess := newStatefulSession()

	// Turn 1: fresh; pauses.
	drainAgent(t, sess, a.Run(runCtx(sess, a, &genai.Content{Parts: []*genai.Part{{Text: "x"}}})), nil)
	if !firstRun.Load() {
		t.Fatal("asker did not run on turn 1")
	}

	// Turn 2: another fresh user message (no FunctionResponse).
	// detectResume should return false; Workflow.Run is invoked.
	a2 := makeAgent(t, workflow.Chain(workflow.Start, makeAsker(&secondRun)))
	drainAgent(t, sess, a2.Run(runCtx(sess, a2, &genai.Content{Parts: []*genai.Part{{Text: "fresh"}}})), nil)
	if !secondRun.Load() {
		t.Error("a fresh user message was misinterpreted as a resume; asker did not run on turn 2")
	}
}
