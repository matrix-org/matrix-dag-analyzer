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

type EventEnumType int64

const (
	AuthEvent EventEnumType = iota
	StateEvent
)

type EventQueue struct {
	queue []*eventNode
}

func NewEventQueue() EventQueue {
	return EventQueue{
		queue: []*eventNode{},
	}
}

func (e *EventQueue) Push(event *eventNode) {
	e.queue = append(e.queue, event)
}

func (e *EventQueue) Pop() {
	e.queue = e.queue[:len(e.queue)-1]
}

func (e *EventQueue) AddChild(eventID EventID, event *eventNode, eventType EventEnumType) {
	for _, queueEvent := range e.queue {
		switch eventType {
		case AuthEvent:
			if _, ok := queueEvent.authChildren[eventID]; !ok {
				queueEvent.authChildren[eventID] = event
			}
		case StateEvent:
			if _, ok := queueEvent.stateChildren[eventID]; !ok {
				queueEvent.stateChildren[eventID] = event
			}
		}
	}
}

func (e *EventQueue) AddChildrenFromNode(event *eventNode, eventType EventEnumType) {
	for _, queueEvent := range e.queue {
		switch eventType {
		case AuthEvent:
			for childID, child := range event.authChildren {
				if _, ok := queueEvent.authChildren[childID]; !ok {
					queueEvent.authChildren[childID] = child
				}
			}
		case StateEvent:
			for childID, child := range event.stateChildren {
				if _, ok := queueEvent.stateChildren[childID]; !ok {
					queueEvent.stateChildren[childID] = child
				}
			}
		}
	}
}
