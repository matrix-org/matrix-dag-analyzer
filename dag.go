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
	"github.com/yourbasic/graph"
)

var newNodeIndex = 0

type eventNode struct {
	event *Event
	index int

	roomChildren map[EventID]*eventNode
	roomParents  map[EventID]*eventNode

	stateChildren     map[EventID]*eventNode
	authChainChildren map[EventID]*eventNode
	authChainParents  map[EventID]*eventNode
}

func newEventNode(event *Event, index int) eventNode {
	return eventNode{
		event:             event,
		index:             index,
		roomChildren:      make(map[EventID]*eventNode),
		roomParents:       make(map[EventID]*eventNode),
		stateChildren:     make(map[EventID]*eventNode),
		authChainChildren: make(map[EventID]*eventNode),
		authChainParents:  make(map[EventID]*eventNode),
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

func (d *RoomDAG) TotalEvents() int {
	return len(d.eventsByID)
}

func (d *RoomDAG) EventsInFile() int {
	eventCount := 0
	for _, event := range d.eventsByID {
		if event.event != nil {
			eventCount = eventCount + 1
		}
	}
	return eventCount
}

func (d *RoomDAG) EventCountByType(eventType string) int {
	return len(d.eventsByType[eventType])
}

func (d *RoomDAG) PrintEventCounts() {
	log.Info().Msg("***************************************************************")
	log.Info().Msg("Event Metrics:")
	authEventCount := 0
	for eventType, events := range d.eventsByType {
		log.Info().Msg(fmt.Sprintf("%s: %d", eventType, len(events)))
		if _, ok := AuthEventTypes[eventType]; ok {
			authEventCount += len(events)
		}
	}
	log.Info().Msg("***************************************************************")
	log.Info().Msg("DAG Metrics:")
	log.Info().Msg(fmt.Sprintf("Auth Events: %d", authEventCount))

	stateEventCount := 0
	parentlessEvents := 0
	parentlessAuthEvents := 0
	forwardExtremities := 0
	authChainGraph := graph.New(d.TotalEvents())
	roomGraph := graph.New(d.TotalEvents())
	for _, node := range d.eventsByID {
		if node.event != nil && node.event.StateKey != nil {
			if node.event.Type == EVENT_TYPE_MEMBER && *node.event.StateKey == "" {
				log.Warn().Msg(fmt.Sprintf("Event: %s of type %s has a zero-length state key", node.event.EventID, node.event.Type))
			}
			stateEventCount += 1
		}
		if node.event != nil && len(node.roomParents) == 0 {
			parentlessEvents = parentlessEvents + 1
		}
		if node.event != nil && len(node.authChainParents) == 0 {
			parentlessAuthEvents = parentlessAuthEvents + 1
		}
		if len(node.roomChildren) == 0 {
			forwardExtremities = forwardExtremities + 1
		}
		for _, child := range node.authChainChildren {
			authChainGraph.Add(node.index, child.index)
		}
		for _, child := range node.roomChildren {
			roomGraph.Add(node.index, child.index)
		}
	}
	log.Info().Msg(fmt.Sprintf("State Events: %d", stateEventCount))

	log.Info().Msg(fmt.Sprintf("Parentless Room Events: %d", parentlessEvents))
	if parentlessEvents != 1 {
		log.Warn().Int("parentless_room_events", parentlessEvents).Msg("There should only be one room event without parents")
	}

	log.Info().Msg(fmt.Sprintf("Parentless Auth Events: %d", parentlessAuthEvents))
	if parentlessAuthEvents != 1 {
		log.Warn().Int("parentless_auth_events", parentlessAuthEvents).Msg("There should only be one auth event without parents")
	}

	log.Info().Msg(fmt.Sprintf("Forward Extremities: %d", forwardExtremities))

	gatherAuthChainMetrics := true
	if !graph.Acyclic(authChainGraph) {
		log.Error().Msg("Auth chain graph is not acyclic, not calculating metrics")
		for _, cycle := range graph.StrongComponents(authChainGraph) {
			log.Error().Msg(fmt.Sprintf("Found Cycle: %v", cycle))
		}
		gatherAuthChainMetrics = false
	}

	if gatherAuthChainMetrics {
		maxAuthChainDepth := calculateAuthChainSize(d.createEvent)
		log.Info().Msg(fmt.Sprintf("(From Create Event): Auth Chain Size: %d, Max Depth: %d", authChainSize, maxAuthChainDepth))
		if authEventCount != authChainSize {
			log.Warn().Msg(fmt.Sprintf("Auth Chain size %d is less than the total amount of auth events (%d)", authChainSize, authEventCount))
		}
	}

	gatherRoomDAGMetrics := true
	if !graph.Acyclic(roomGraph) {
		log.Error().Msg("Room graph is not acyclic, not calculating metrics")
		for _, cycle := range graph.StrongComponents(roomGraph) {
			log.Error().Msg(fmt.Sprintf("Found Cycle: %v", cycle))
		}
		gatherRoomDAGMetrics = false
	}

	if gatherRoomDAGMetrics {
		//stats := graph.Check(roomGraph)
		//statsTranspose := graph.Check(graph.Transpose(roomGraph))
		//log.Info().Msg(fmt.Sprintf("Dangling parents: %d", statsTranspose.Isolated))
		//log.Info().Msg(fmt.Sprintf("Dangling children: %d", stats.Isolated))

		maxRoomDepth := calculateRoomDAGSize(d.createEvent.event.EventID, d.createEvent)
		log.Info().Msg(fmt.Sprintf("(From Create Event): Room DAG Size: %d, Max Depth: %d", roomDAGSize, maxRoomDepth))
		totalEventCount := d.EventsInFile()
		if totalEventCount != roomDAGSize {
			log.Warn().Int("missing_events", totalEventCount-roomDAGSize).Msg(fmt.Sprintf("Room DAG size (%d) is less than the amount of events in the file (%d)", roomDAGSize, totalEventCount))
		}
	}

	// NOTE: uncomment this to see those events that aren't found when walking the roomDAG from the create event
	//missingEvents := map[EventID]struct{}{}
	//for id, event := range d.eventsByID {
	//	if event.event != nil {
	//		if _, ok := roomDAGSeenEvents[id]; !ok {
	//			missingEvents[id] = struct{}{}
	//		}
	//	}
	//}
	//log.Warn().Msg(fmt.Sprintf("Missing Events: %v", missingEvents))

	log.Info().Msg("***************************************************************")
}

var authChainSize = 0
var authChainSeenEvents = map[EventID]struct{}{}

func calculateAuthChainSize(event *eventNode) int {
	maxDepth := 0
	if _, ok := authChainSeenEvents[event.event.EventID]; !ok {
		authChainSize = authChainSize + 1
	}
	authChainSeenEvents[event.event.EventID] = struct{}{}
	for _, child := range event.authChainChildren {
		maxDepth = int(math.Max(float64(maxDepth), float64(calculateAuthChainSize(child))))
	}

	return maxDepth + 1
}

type roomChain struct {
	roomChain []EventID
}

func (r *roomChain) contains(eventID EventID) bool {
	for _, id := range r.roomChain {
		if eventID == id {
			return true
		}
	}
	return false
}

func (r *roomChain) push(eventID EventID) {
	r.roomChain = append(r.roomChain, eventID)
}

func (r *roomChain) pop() {
	r.roomChain = r.roomChain[:len(r.roomChain)-1]
}

func (r *roomChain) prevInstanceOf(eventID EventID) []EventID {
	index := 0
	for i, id := range r.roomChain {
		if eventID == id {
			index = i
			break
		}
	}

	return r.roomChain[index:]
}

var chain = roomChain{
	roomChain: []EventID{},
}

var roomDAGSize = 0
var roomDAGSeenEvents = map[EventID]struct{}{}

func calculateRoomDAGSize(eventID EventID, event *eventNode) int {
	maxDepth := 0
	if _, ok := roomDAGSeenEvents[eventID]; !ok {
		roomDAGSize = roomDAGSize + 1
		roomDAGSeenEvents[eventID] = struct{}{}
	} else {
		if chain.contains(eventID) {
			log.Error().
				Str("loop", fmt.Sprintf("%v", append(chain.prevInstanceOf(eventID), eventID))).
				Msg("Loop Detected!")
		}
		return maxDepth
	}

	chain.push(eventID)

	for childID, child := range event.roomChildren {
		maxDepth = int(math.Max(float64(maxDepth), float64(calculateRoomDAGSize(childID, child))))
	}

	chain.pop()

	return maxDepth + 1
}

func (d *RoomDAG) addEvent(newEvent Event) error {
	if foundEvent, ok := d.eventsByID[newEvent.EventID]; ok && foundEvent.event != nil {
		return fmt.Errorf("Duplicate event ID detected in file: %s", newEvent.EventID)
	}
	if d.roomID != nil && *d.roomID != newEvent.RoomID {
		return fmt.Errorf("Received event with different room ID. Expected: %s, Got: %s", *d.roomID, newEvent.RoomID)
	}
	if d.createEvent != nil && newEvent.Type == EVENT_TYPE_CREATE {
		return fmt.Errorf("More than 1 create event present. %s & %s", d.createEvent.event.EventID, newEvent.EventID)
	}
	if d.roomID == nil {
		d.roomID = &newEvent.RoomID
	}

	if _, ok := d.eventsByID[newEvent.EventID]; !ok {
		newNode := newEventNode(&newEvent, newNodeIndex)
		newNodeIndex = newNodeIndex + 1
		d.eventsByID[newEvent.EventID] = &newNode
	}
	newNode := d.eventsByID[newEvent.EventID]
	newNode.event = &newEvent

	if newEvent.Type == EVENT_TYPE_CREATE {
		d.createEvent = newNode
	}

	if events, ok := d.eventsByType[newEvent.Type]; ok {
		d.eventsByType[newEvent.Type] = append(events, newNode)
	} else {
		d.eventsByType[newEvent.Type] = []*eventNode{newNode}
	}

	for _, authEventID := range newEvent.AuthEvents {
		if _, ok := d.eventsByID[authEventID]; !ok {
			// NOTE: add a placeholder event
			newNode := newEventNode(nil, newNodeIndex)
			newNodeIndex = newNodeIndex + 1
			d.eventsByID[authEventID] = &newNode
		}
		authNode := d.eventsByID[authEventID]

		// NOTE: Populate the auth chains for the room
		if _, ok := AuthEventTypes[newEvent.Type]; ok {
			if _, ok := authNode.authChainChildren[newEvent.EventID]; !ok {
				authNode.authChainChildren[newEvent.EventID] = newNode
			}
		}

		if _, ok := newNode.authChainParents[authEventID]; !ok {
			newNode.authChainParents[authEventID] = authNode
		}
	}

	for _, prevEventID := range newEvent.PrevEvents {
		if _, ok := d.eventsByID[prevEventID]; !ok {
			// NOTE: add a placeholder event
			newNode := newEventNode(nil, newNodeIndex)
			newNodeIndex = newNodeIndex + 1
			d.eventsByID[prevEventID] = &newNode
		}
		prevNode := d.eventsByID[prevEventID]

		// NOTE: Populate the room DAG
		if _, ok := prevNode.roomChildren[newEvent.EventID]; !ok {
			prevNode.roomChildren[newEvent.EventID] = newNode
		}

		if _, ok := newNode.roomParents[prevEventID]; !ok {
			newNode.roomParents[prevEventID] = prevNode
		}
	}

	// TODO: Create the state DAG subset
	// TODO: Create the auth DAG subset

	return nil
}
