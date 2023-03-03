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
	event      *Event
	index      int
	stateIndex *int

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
		stateIndex:        nil,
		roomChildren:      make(map[EventID]*eventNode),
		roomParents:       make(map[EventID]*eventNode),
		stateChildren:     make(map[EventID]*eventNode),
		authChainChildren: make(map[EventID]*eventNode),
		authChainParents:  make(map[EventID]*eventNode),
	}
}

func (e *eventNode) isStateEvent() bool {
	return e.event != nil && e.event.StateKey != nil
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
		if IsAuthEvent(eventType) {
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
			index := stateEventCount
			node.stateIndex = &index
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

	gatherAuthChainMetrics := true
	if !graph.Acyclic(authChainGraph) {
		log.Error().Msg("Auth chain graph is not acyclic, not calculating metrics")
		for _, cycle := range graph.StrongComponents(authChainGraph) {
			log.Error().Msg(fmt.Sprintf("Found Cycle: %v", cycle))
		}
		gatherAuthChainMetrics = false
	}

	if gatherAuthChainMetrics {
		maxAuthChainDepth := traverseAuthChains(d.createEvent)
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
		for nodeID := range d.eventsByID {
			roomDAGDepth[nodeID] = 0
		}
		for nodeID, node := range d.eventsByID {
			chain = newRoomChain()
			//traverseRoomDAG(d.createEvent.event.EventID, d.createEvent)
			traverseRoomDAG(nodeID, node)
		}

		maxRoomDepth := 0
		for _, depth := range roomDAGDepth {
			maxRoomDepth = int(math.Max(float64(maxRoomDepth), float64(depth)))
		}

		totalEventCount := d.EventsInFile()
		if totalEventCount != roomDAGSize {
			log.Warn().Int("missing_events", totalEventCount-roomDAGSize).Msg(fmt.Sprintf("Room DAG size (%d) is less than the amount of events in the file (%d)", roomDAGSize, totalEventCount))
		}

		stateDAGForwardExtremities := 0
		stateChildCount := map[int]int{}
		roomChildCount := map[int]int{}
		stateGraph := graph.New(stateEventCount)
		for _, node := range d.eventsByID {
			if node.isStateEvent() {
				for _, child := range node.stateChildren {
					stateGraph.Add(*node.stateIndex, *child.stateIndex)
				}
				count, ok := stateChildCount[len(node.stateChildren)]
				if !ok {
					stateChildCount[len(node.stateChildren)] = 1
				} else {
					stateChildCount[len(node.stateChildren)] = count + 1
				}
				if len(node.stateChildren) == 0 {
					stateDAGForwardExtremities = stateDAGForwardExtremities + 1
				}
			}
			count, ok := roomChildCount[len(node.roomChildren)]
			if !ok {
				roomChildCount[len(node.roomChildren)] = 1
			} else {
				roomChildCount[len(node.roomChildren)] = count + 1
			}
		}

		if !graph.Acyclic(stateGraph) {
			log.Error().Msg("State graph is not acyclic, not calculating metrics")
			for _, cycle := range graph.StrongComponents(stateGraph) {
				log.Error().Msg(fmt.Sprintf("Found Cycle: %v", cycle))
			}
		}

		stats := graph.Check(roomGraph)
		statsTranspose := graph.Check(graph.Transpose(roomGraph))
		log.Info().Msg(fmt.Sprintf("Dangling Room parents: %d", statsTranspose.Isolated))
		log.Info().Msg(fmt.Sprintf("Dangling Room children: %d", stats.Isolated))

		statsState := graph.Check(stateGraph)
		statsStateTranspose := graph.Check(graph.Transpose(stateGraph))
		log.Info().Msg(fmt.Sprintf("Dangling State parents: %d", statsStateTranspose.Isolated))
		log.Info().Msg(fmt.Sprintf("Dangling State children: %d", statsState.Isolated))

		log.Info().Msg(fmt.Sprintf("Room Edges: %d", stats.Size))
		log.Info().Msg(fmt.Sprintf("State Edges: %d", statsState.Size))

		// NOTE: The room DAG has deterministic children counts
		log.Info().Msg(fmt.Sprintf("Room DAG Child Count [# of children: # of nodes]: %v", roomChildCount))
		// FIX: The state DAG does not have deterministic children counts
		log.Info().Msg(fmt.Sprintf("State DAG Child Count [# of children: # of nodes]: %v", stateChildCount))
		log.Info().Msg(fmt.Sprintf("Forward Extremities (Room): %d", forwardExtremities))
		log.Info().Msg(fmt.Sprintf("Forward Extremities (State DAG): %d", stateDAGForwardExtremities))

		for nodeID, node := range d.eventsByID {
			if node.isStateEvent() {
				stateDAGDepth[nodeID] = 0
			}
		}
		for _, node := range d.eventsByID {
			if node.isStateEvent() {
				//traverseStateDAG(d.createEvent)
				traverseStateDAG(node)
			}
		}

		maxStateDepth := 0
		for _, depth := range stateDAGDepth {
			maxStateDepth = int(math.Max(float64(maxStateDepth), float64(depth)))
		}

		log.Info().Msg(fmt.Sprintf("(From Create Event): Room DAG Size: %d, Max Depth: %d, Forks: %d", roomDAGSize, maxRoomDepth, roomDAGForks))
		log.Info().Msg(fmt.Sprintf("(From Create Event): State DAG Size: %d, Max Depth: %d, Forks: %d", stateDAGSize, maxStateDepth, stateDAGForks))

		// TODO: Create the auth DAG subset
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

var stateDAGSize = 0
var stateDAGForks = 0
var stateDAGSeenEvents = map[EventID]struct{}{}

// NOTE: must init all events to 0
var stateDAGDepth = map[EventID]int{}

func traverseStateDAG(event *eventNode) {
	if _, ok := stateDAGSeenEvents[event.event.EventID]; !ok {
		stateDAGSize = stateDAGSize + 1
		if len(event.stateChildren) > 1 {
			stateDAGForks = stateDAGForks + 1
		}
	}

	stateDAGSeenEvents[event.event.EventID] = struct{}{}
	for childID, child := range event.stateChildren {
		if _, ok := stateDAGSeenEvents[childID]; !ok {
			traverseStateDAG(child)
		}

		stateDAGDepth[event.event.EventID] = int(math.Max(float64(stateDAGDepth[child.event.EventID]), float64(1+stateDAGDepth[childID])))
	}
}

var authChainSize = 0
var authChainSeenEvents = map[EventID]struct{}{}

func traverseAuthChains(event *eventNode) int {
	maxDepth := 0
	if _, ok := authChainSeenEvents[event.event.EventID]; !ok {
		authChainSize = authChainSize + 1
	}
	authChainSeenEvents[event.event.EventID] = struct{}{}
	for _, child := range event.authChainChildren {
		maxDepth = int(math.Max(float64(maxDepth), float64(traverseAuthChains(child))))
	}

	return maxDepth + 1
}

type roomChain struct {
	roomChain []*eventNode
}

func newRoomChain() roomChain {
	return roomChain{
		roomChain: []*eventNode{},
	}
}

func (r *roomChain) contains(eventID EventID) bool {
	for _, event := range r.roomChain {
		if eventID == event.event.EventID {
			return true
		}
	}
	return false
}

func (r *roomChain) push(event *eventNode) {
	r.roomChain = append(r.roomChain, event)
}

func (r *roomChain) pop() {
	r.roomChain = r.roomChain[:len(r.roomChain)-1]
}

func (r *roomChain) getEventsFromPrevInstanceOf(eventID EventID) []*eventNode {
	index := 0
	for i, event := range r.roomChain {
		if eventID == event.event.EventID {
			index = i
			break
		}
	}

	return r.roomChain[index:]
}

func (r *roomChain) getLastStateEvent() *eventNode {
	for i := len(r.roomChain) - 1; i >= 0; i-- {
		if r.roomChain[i].isStateEvent() {
			return r.roomChain[i]
		}
	}
	//log.Warn().Msg("Cannot find a previous state event...")
	return nil
}

var chain = newRoomChain()
var roomDAGSize = 0
var roomDAGForks = 0
var roomDAGSeenEvents = map[EventID]struct{}{}

// NOTE: must init all events to 0
var roomDAGDepth = map[EventID]int{}

func traverseRoomDAG(eventID EventID, event *eventNode) {
	// FIX: This doesn't produce deterministic state DAGs
	if event.isStateEvent() {
		lastStateEvent := chain.getLastStateEvent()
		if lastStateEvent != nil {
			if _, ok := lastStateEvent.stateChildren[eventID]; !ok {
				lastStateEvent.stateChildren[eventID] = event
			}
		}
	}

	if _, ok := roomDAGSeenEvents[eventID]; !ok {
		roomDAGSize = roomDAGSize + 1
		roomDAGSeenEvents[eventID] = struct{}{}
		if len(event.roomChildren) > 1 {
			roomDAGForks = roomDAGForks + 1
		}
	}

	chain.push(event)

	for childID, child := range event.roomChildren {
		// Only traverse children we haven't already seen
		if _, ok := roomDAGSeenEvents[childID]; !ok {
			traverseRoomDAG(childID, child)
		}

		roomDAGDepth[eventID] = int(math.Max(float64(roomDAGDepth[eventID]), float64(1+roomDAGDepth[childID])))
	}

	chain.pop()
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
		if IsAuthEvent(newEvent.Type) {
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

	return nil
}
