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

package workflow

import (
	"errors"
	"fmt"
	"iter"

	"github.com/google/jsonschema-go/jsonschema"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/internal/typeutil"
	"google.golang.org/adk/session"
)

// ErrInvalidResumeResponse is returned by Workflow.Resume when a
// response payload does not match its corresponding
// RequestInput.ResponseSchema. The waiting node is left in
// NodeWaiting with PendingRequest intact so the caller can retry
// with a corrected payload.
var ErrInvalidResumeResponse = errors.New("workflow: resume response does not match request schema")

// Resume continues a previously paused workflow run. state is the
// RunState loaded from session storage; responses maps
// RequestInput.InterruptID to the user-supplied response payload.
//
// For each waiting node whose InterruptID has a matching entry in
// responses, Resume:
//
//  1. Validates the payload against PendingRequest.ResponseSchema,
//     if non-nil. A mismatch surfaces as ErrInvalidResumeResponse
//     via the iterator and leaves the node in NodeWaiting with
//     PendingRequest intact.
//
//  2. Consumes the pending request (clears PendingRequest, sets
//     Status = NodePending) before re-scheduling, so a duplicate
//     Resume call with the same InterruptID becomes a no-op.
//
//  3. Routes the response to the asker's successors as if the
//     asker had emitted it as its output (handoff mode). The
//     asker itself does NOT re-execute.
//
// Waiting nodes whose InterruptID is absent from responses remain
// in NodeWaiting unchanged.
//
// Resume is a no-op (returns immediately, yields nothing) if no
// waiting node matches an entry in responses. This guard prevents
// scheduler.run from blocking forever on an empty event queue
// were it entered with no scheduled activations.
func (w *Workflow) Resume(
	ctx agent.InvocationContext,
	state *RunState,
	responses map[string]any,
) iter.Seq2[*session.Event, error] {
	return func(yield func(*session.Event, error) bool) {
		if state == nil || len(responses) == 0 {
			return
		}

		s := newSchedulerFromState(ctx, w.graph, state)

		scheduled := 0
		for name, ns := range state.Nodes {
			if ns.Status != NodeWaiting || ns.PendingRequest == nil {
				continue
			}
			resp, ok := responses[ns.PendingRequest.InterruptID]
			if !ok {
				continue
			}

			// Schema validation: surface a typed error and leave
			// the node parked so the caller can retry.
			if ns.PendingRequest.ResponseSchema != nil {
				validated, err := validateResumeResponse(resp, ns.PendingRequest.ResponseSchema)
				if err != nil {
					if !yield(nil, fmt.Errorf("%w: node %q: %v", ErrInvalidResumeResponse, name, err)) {
						return
					}
					continue
				}
				resp = validated
			}

			node := s.nodesByName[name]
			if node == nil {
				continue
			}

			// Consume PendingRequest before scheduling. A duplicate
			// Resume with the same InterruptID will skip this node
			// because PendingRequest is now nil.
			ns.PendingRequest = nil
			ns.Status = NodePending

			// Handoff mode: schedule each successor with the
			// response as its input, exactly as if the asker had
			// emitted it as output. Reuses findSuccessors so
			// routing, fan-out and fan-in invariants apply
			// uniformly. (No routing event is supplied, so a
			// router-style handoff target falls back to its
			// Default route or dead-ends.)
			for _, succ := range findSuccessors(s.graph, node, resp, nil) {
				s.scheduleNode(succ.node, succ.input, succ.triggeredBy)
			}
			scheduled++
		}

		if scheduled == 0 {
			return
		}

		s.run(yield)
		s.wg.Wait()

		// Persist the post-resume state via a session.Event with
		// StateDelta. If new nodes paused during this Resume the
		// next turn will see them; if the run completed the state
		// reflects that too.
		yieldRunStateEvent(ctx, w.name, s.state, yield)
	}
}

// newSchedulerFromState constructs a scheduler seeded with an
// existing RunState (loaded from session) instead of a fresh one.
// Used by Resume to pick up bookkeeping for nodes that already
// have a per-node history (NodeCompleted, NodeWaiting, etc.).
func newSchedulerFromState(parent agent.InvocationContext, g *graph, state *RunState) *scheduler {
	s := newScheduler(parent, g)
	if state != nil {
		s.state = state
	}
	return s
}

// validateResumeResponse coerces resp into the type described by
// schema, returning the validated value or an error. When schema
// is nil, resp passes through unchanged.
//
// Delegates to internal/typeutil.ConvertToWithJSONSchema, the same
// helper FunctionNode uses to coerce node inputs. Resolves the
// schema on each call rather than caching: PR #2's persistence
// path round-trips schemas through JSON without preserving any
// resolved form.
func validateResumeResponse(resp any, schema *jsonschema.Schema) (any, error) {
	if schema == nil {
		return resp, nil
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		return nil, fmt.Errorf("resolve schema: %w", err)
	}
	return typeutil.ConvertToWithJSONSchema[any, any](resp, resolved)
}
