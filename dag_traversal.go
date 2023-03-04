// Copyright 2023 The Matrix.org Foundation C.I.C.
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

package analyzer

import "math"

// TODO: Refactor these common functions
func generateStateDAG(eventID EventID, event *EventNode, origin *EventNode, queue *EventQueue, metrics *DAGGenerationMetrics) {
	metrics.seenEvents[eventID] = struct{}{}

	if event != origin && event.isStateEvent() {
		queue.AddChild(eventID, event, StateEvent)
		return
	}

	queue.Push(event)
	for childID, child := range event.roomChildren {
		// Only traverse children we haven't already seen
		if _, ok := metrics.seenEvents[childID]; !ok {
			generateStateDAG(childID, child, origin, queue, metrics)
		} else {
			if child.isStateEvent() {
				queue.AddChild(childID, child, StateEvent)
			} else {
				queue.AddChildrenFromNode(child, StateEvent)
			}
		}
	}
	queue.Pop()
}

func generateAuthDAG(eventID EventID, event *EventNode, origin *EventNode, queue *EventQueue, metrics *DAGGenerationMetrics) {
	metrics.seenEvents[eventID] = struct{}{}

	if event != origin && event.isAuthEvent() {
		queue.AddChild(eventID, event, AuthEvent)
		return
	}

	queue.Push(event)
	for childID, child := range event.roomChildren {
		// Only traverse children we haven't already seen
		if _, ok := metrics.seenEvents[childID]; !ok {
			generateAuthDAG(childID, child, origin, queue, metrics)
		} else {
			if child.isAuthEvent() {
				queue.AddChild(childID, child, AuthEvent)
			} else {
				queue.AddChildrenFromNode(child, AuthEvent)
			}
		}
	}
	queue.Pop()
}

// TODO: Refactor these common functions
func traverseRoomDAG(eventID EventID, event *EventNode, metrics *DAGTraversalMetrics) {
	if _, ok := metrics.seenEvents[eventID]; !ok {
		metrics.size += 1
		metrics.seenEvents[eventID] = struct{}{}
		if len(event.roomChildren) > 1 {
			metrics.forks += 1
		}
	}

	for childID, child := range event.roomChildren {
		// Only traverse children we haven't already seen
		if _, ok := metrics.seenEvents[childID]; !ok {
			traverseRoomDAG(childID, child, metrics)
		}

		metrics.depths[eventID] = int(math.Max(
			float64(metrics.depths[eventID]),
			float64(1+metrics.depths[childID]),
		))
	}
}

func traverseAuthChain(event *EventNode, metrics *DAGTraversalMetrics) {
	if _, ok := metrics.seenEvents[event.event.EventID]; !ok {
		metrics.size += 1
		metrics.seenEvents[event.event.EventID] = struct{}{}
		if len(event.authChainChildren) > 1 {
			metrics.forks += 1
		}
	}

	for childID, child := range event.authChainChildren {
		// Only traverse children we haven't already seen
		if _, ok := metrics.seenEvents[childID]; !ok {
			traverseAuthChain(child, metrics)
		}

		metrics.depths[event.event.EventID] = int(math.Max(
			float64(metrics.depths[event.event.EventID]),
			float64(1+metrics.depths[childID]),
		))
	}
}

func traverseStateDAG(event *EventNode, metrics *DAGTraversalMetrics) {
	if _, ok := metrics.seenEvents[event.event.EventID]; !ok {
		metrics.size += 1
		metrics.seenEvents[event.event.EventID] = struct{}{}
		if len(event.stateChildren) > 1 {
			metrics.forks += 1
		}
	}

	for childID, child := range event.stateChildren {
		// Only traverse children we haven't already seen
		if _, ok := metrics.seenEvents[childID]; !ok {
			traverseStateDAG(child, metrics)
		}

		metrics.depths[event.event.EventID] = int(math.Max(
			float64(metrics.depths[event.event.EventID]),
			float64(1+metrics.depths[childID]),
		))
	}
}

func traverseAuthDAG(event *EventNode, metrics *DAGTraversalMetrics) {
	if _, ok := metrics.seenEvents[event.event.EventID]; !ok {
		metrics.size += 1
		metrics.seenEvents[event.event.EventID] = struct{}{}
		if len(event.authChildren) > 1 {
			metrics.forks += 1
		}
	}

	for childID, child := range event.authChildren {
		// Only traverse children we haven't already seen
		if _, ok := metrics.seenEvents[childID]; !ok {
			traverseAuthDAG(child, metrics)
		}

		metrics.depths[event.event.EventID] = int(math.Max(
			float64(metrics.depths[event.event.EventID]),
			float64(1+metrics.depths[childID]),
		))
	}
}
