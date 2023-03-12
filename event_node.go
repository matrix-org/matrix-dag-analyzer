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

type EventNode struct {
	event      *Event
	roomIndex  int
	authIndex  *int
	stateIndex *int
	powerIndex *int

	roomChildren map[EventID]*EventNode
	roomParents  map[EventID]*EventNode

	stateChildren     map[EventID]*EventNode
	authChildren      map[EventID]*EventNode
	authChainChildren map[EventID]*EventNode
	authChainParents  map[EventID]*EventNode

	powerChildren    map[EventID]*EventNode
	linearPowerIndex *int
}

type StateEventNode *EventNode
type PowerEventNode *EventNode
type TimelineEventNode *EventNode

type StateEventLinks struct {
	PrevPowerEvent  *PowerEventNode
	PrevStateEvents []*StateEventNode
}

type PowerEventLinks struct {
	PrevPowerEvent *PowerEventNode
}

type TimelineEventLinks struct {
	PrevPowerEvent     *PowerEventNode
	PrevTimelineEvents []*TimelineEventNode
}

func newEventNode(event *Event, index int) EventNode {
	return EventNode{
		event:             event,
		roomIndex:         index,
		authIndex:         nil,
		stateIndex:        nil,
		roomChildren:      make(map[EventID]*EventNode),
		roomParents:       make(map[EventID]*EventNode),
		stateChildren:     make(map[EventID]*EventNode),
		authChildren:      make(map[EventID]*EventNode),
		authChainChildren: make(map[EventID]*EventNode),
		authChainParents:  make(map[EventID]*EventNode),
		powerChildren:     make(map[EventID]*EventNode),
	}
}

func (e *EventNode) isStateEvent() bool {
	return e.event != nil && e.event.StateKey != nil
}

func (e *EventNode) isAuthEvent() bool {
	return e.event != nil && IsAuthEvent(e.event.Type)
}

func (e *EventNode) isPowerEvent() bool {
	return e.event != nil && IsPowerEvent(e.event)
}

func (e *EventNode) isNewStateEvent() bool {
	return e.isStateEvent() && !e.isPowerEvent()
}
