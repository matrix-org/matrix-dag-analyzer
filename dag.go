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

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"

	"github.com/rs/zerolog/log"
)

type eventNode struct {
	event         *Event
	stateChildren map[EventID]*eventNode
	authChildren  map[EventID]*eventNode
}

func newEventNode(event *Event) eventNode {
	return eventNode{
		event:         event,
		stateChildren: make(map[EventID]*eventNode),
		authChildren:  make(map[EventID]*eventNode),
	}
}

type RoomDAG struct {
	eventsByID   map[EventID]*eventNode
	eventsByType map[EventType][]*eventNode

	createEvent *eventNode
	roomID      *string
}

func NewRoomDAG() RoomDAG {
	return RoomDAG{
		eventsByID:   make(map[EventID]*eventNode),
		eventsByType: make(map[EventType][]*eventNode),
		createEvent:  nil,
		roomID:       nil,
	}
}

func ParseDAGFromFile(filename string) (*RoomDAG, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	dag := NewRoomDAG()
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber = lineNumber + 1
		var event Event
		err := json.Unmarshal([]byte(scanner.Text()), &event)
		if err != nil {
			return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
		}

		err = dag.addEvent(event)
		if err != nil {
			return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
		}
	}

	return &dag, nil
}

func (d *RoomDAG) TotalEventCount() int {
	return len(d.eventsByID)
}

func (d *RoomDAG) EventCountByType(eventType string) int {
	return len(d.eventsByType[eventType])
}

func (d *RoomDAG) PrintEventCounts() {
	log.Info().Msg("Total Event Counts:")
	authEventCount := 0
	for eventType, events := range d.eventsByType {
		log.Info().Msg(fmt.Sprintf("%s: %d", eventType, len(events)))
		if _, ok := AuthEventTypes[eventType]; ok {
			authEventCount += len(events)
		}
	}
	log.Info().Msg(fmt.Sprintf("Auth Events: %d", authEventCount))

	stateEventCount := 0
	for _, event := range d.eventsByID {
		if event.event != nil && event.event.StateKey != nil {
			if event.event.Type == EVENT_TYPE_MEMBER && *event.event.StateKey == "" {
				log.Warn().Msg(fmt.Sprintf("Event: %s of type %s has a zero-length state key", event.event.EventID, event.event.Type))
			}
			stateEventCount += 1
		}
	}
	log.Info().Msg(fmt.Sprintf("State Events: %d", stateEventCount))

	maxAuthDepth := calculateAuthDAGSize(d.createEvent)
	log.Info().Msg(fmt.Sprintf("(Since Create): Auth DAG Size: %d, Max Depth: %d", authChainSize, maxAuthDepth))
	maxStateDepth := calculateStateDAGSize(d.createEvent)
	log.Info().Msg(fmt.Sprintf("(Since Create): State DAG Size: %d, Max Depth: %d", stateChainSize, maxStateDepth))
}

var authChainSize = 0
var stateChainSize = 0
var authSeenEvents = map[EventID]struct{}{}
var stateSeenEvents = map[EventID]struct{}{}

func calculateAuthDAGSize(event *eventNode) int {
	maxDepth := 0
	if _, ok := authSeenEvents[event.event.EventID]; !ok {
		authChainSize = authChainSize + 1
	}
	authSeenEvents[event.event.EventID] = struct{}{}
	for _, child := range event.authChildren {
		maxDepth = int(math.Max(float64(maxDepth), float64(calculateAuthDAGSize(child))))
	}

	return maxDepth + 1
}

func calculateStateDAGSize(event *eventNode) int {
	maxDepth := 0
	if _, ok := stateSeenEvents[event.event.EventID]; !ok {
		stateChainSize = stateChainSize + 1
	}
	stateSeenEvents[event.event.EventID] = struct{}{}
	for _, child := range event.stateChildren {
		maxDepth = int(math.Max(float64(maxDepth), float64(calculateStateDAGSize(child))))
	}

	return maxDepth + 1
}

func (d *RoomDAG) addEvent(newEvent Event) error {
	if foundEvent, ok := d.eventsByID[newEvent.EventID]; ok && foundEvent.event != nil {
		return fmt.Errorf("Duplicate event ID detected in file: %s", newEvent.EventID)
	}
	if d.roomID != nil && *d.roomID != newEvent.RoomID {
		return fmt.Errorf("Received event with different room ID. Expected: %s, Got: %s", *d.roomID, newEvent.RoomID)
	}
	if d.roomID == nil {
		d.roomID = &newEvent.RoomID
	}

	if _, ok := d.eventsByID[newEvent.EventID]; !ok {
		newNode := newEventNode(&newEvent)
		d.eventsByID[newEvent.EventID] = &newNode
	}
	d.eventsByID[newEvent.EventID].event = &newEvent

	if newEvent.Type == EVENT_TYPE_CREATE {
		d.createEvent = d.eventsByID[newEvent.EventID]
	}

	if events, ok := d.eventsByType[newEvent.Type]; ok {
		d.eventsByType[newEvent.Type] = append(events, d.eventsByID[newEvent.EventID])
	} else {
		d.eventsByType[newEvent.Type] = []*eventNode{d.eventsByID[newEvent.EventID]}
	}

	for _, authEvent := range newEvent.AuthEvents {
		if _, ok := d.eventsByID[authEvent]; !ok {
			// NOTE: add a placeholder event
			newNode := newEventNode(nil)
			d.eventsByID[authEvent] = &newNode
		}
		event := d.eventsByID[authEvent]

		if _, ok := AuthEventTypes[newEvent.Type]; ok {
			if _, ok := event.authChildren[newEvent.EventID]; !ok {
				event.authChildren[newEvent.EventID] = d.eventsByID[newEvent.EventID]
			}
		}

		if event.event != nil && event.event.StateKey != nil {
			if _, ok := event.stateChildren[newEvent.EventID]; !ok {
				event.stateChildren[newEvent.EventID] = d.eventsByID[newEvent.EventID]
			}
		}
	}

	for _, prevEvent := range newEvent.PrevEvents {
		if _, ok := d.eventsByID[prevEvent]; !ok {
			// NOTE: add a placeholder event
			newNode := newEventNode(nil)
			d.eventsByID[prevEvent] = &newNode
		}
		event := d.eventsByID[prevEvent]

		if event.event != nil && event.event.StateKey != nil {
			if _, ok := event.stateChildren[newEvent.EventID]; !ok {
				event.stateChildren[newEvent.EventID] = d.eventsByID[newEvent.EventID]
			}
		}
	}

	return nil
}
