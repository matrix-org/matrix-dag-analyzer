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
	event          *Event
	roomIndex      int
	authChainIndex *int
	stateIndex     *int

	roomChildren map[EventID]*eventNode
	roomParents  map[EventID]*eventNode

	stateChildren     map[EventID]*eventNode
	authChainChildren map[EventID]*eventNode
	authChainParents  map[EventID]*eventNode
}

func newEventNode(event *Event, index int) eventNode {
	return eventNode{
		event:             event,
		roomIndex:         index,
		authChainIndex:    nil,
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

func (e *eventNode) isAuthEvent() bool {
	return e.event != nil && IsAuthEvent(e.event.Type)
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

func (d *RoomDAG) GenerateMetrics() {
	log.Info().Msg("***************************************************************")
	log.Info().Msg("Event Metrics:")
	for eventType, events := range d.eventsByType {
		log.Info().Msg(fmt.Sprintf("%s: %d", eventType, len(events)))
	}
	log.Info().Msg("***************************************************************")

	stateEventCount := 0
	authEventCount := 0
	roomGraph := graph.New(d.TotalEvents())
	for nodeID, node := range d.eventsByID {
		roomDAGDepths[nodeID] = 0
		if node.isStateEvent() {
			if node.event.Type == EVENT_TYPE_MEMBER && *node.event.StateKey == "" {
				log.Warn().Msg(fmt.Sprintf("Event: %s of type %s has a zero-length state key", node.event.EventID, node.event.Type))
			}
			index := stateEventCount
			node.stateIndex = &index
			stateEventCount += 1
			stateDAGDepths[nodeID] = 0
		}

		if node.isAuthEvent() {
			index := authEventCount
			node.authChainIndex = &index
			authEventCount += 1
			authChainDepths[nodeID] = 0
		}

		for _, child := range node.roomChildren {
			roomGraph.Add(node.roomIndex, child.roomIndex)
		}
	}

	if !graph.Acyclic(roomGraph) {
		log.Error().Msg("Room graph is not acyclic, not calculating metrics")
		for _, cycle := range graph.StrongComponents(roomGraph) {
			log.Error().Msg(fmt.Sprintf("Found Cycle: %v", cycle))
		}
		return
	}

	authChainGraph := graph.New(authEventCount)
	for _, node := range d.eventsByID {
		if node.isAuthEvent() {
			for _, child := range node.authChainChildren {
				authChainGraph.Add(*node.authChainIndex, *child.authChainIndex)
			}
		}
	}
	if !graph.Acyclic(authChainGraph) {
		log.Error().Msg("Auth chain graph is not acyclic, not calculating metrics")
		for _, cycle := range graph.StrongComponents(authChainGraph) {
			log.Error().Msg(fmt.Sprintf("Found Cycle: %v", cycle))
		}
		return
	}

	log.Info().Msg("Calculating Room DAG Metrics...")
	for nodeID, node := range d.eventsByID {
		traverseRoomDAG(nodeID, node)
	}

	log.Info().Msg("Calculating Auth Chain Metrics...")
	traverseAuthChain(d.createEvent)
	if authEventCount != authChainSize {
		log.Warn().Msg(fmt.Sprintf("Auth Chain size %d is less than the total amount of auth events (%d)", authChainSize, authEventCount))
	}

	maxAuthChainDepth := 0
	for _, depth := range authChainDepths {
		maxAuthChainDepth = int(math.Max(float64(maxAuthChainDepth), float64(depth)))
	}

	log.Info().Msg("Generating State DAG...")
	for nodeID, node := range d.eventsByID {
		queue := NewEventQueue()
		createStateDAG(nodeID, node, node, &queue)
	}

	log.Info().Msg("Calculating State DAG Metrics...")
	for _, node := range d.eventsByID {
		if node.isStateEvent() {
			traverseStateDAG(node)
		}
	}

	log.Info().Msg("Generating Auth DAG...")
	// TODO: generate the auth DAG

	log.Info().Msg("Calculating Auth DAG Metrics...")
	// TODO: walk the auth DAG

	maxRoomDepth := 0
	for _, depth := range roomDAGDepths {
		maxRoomDepth = int(math.Max(float64(maxRoomDepth), float64(depth)))
	}

	maxStateDepth := 0
	for _, depth := range stateDAGDepths {
		maxStateDepth = int(math.Max(float64(maxStateDepth), float64(depth)))
	}

	roomChildCount := map[int]int{}
	authChainChildCount := map[int]int{}
	stateChildCount := map[int]int{}
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
		}

		if node.isAuthEvent() {
			count, ok := authChainChildCount[len(node.authChainChildren)]
			if !ok {
				authChainChildCount[len(node.authChainChildren)] = 1
			} else {
				authChainChildCount[len(node.authChainChildren)] = count + 1
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
		return
	}

	log.Info().Msg("***************************************************************")
	log.Info().Msg("DAG Metrics:")
	log.Info().Msg(fmt.Sprintf("Room Events: %d", d.TotalEvents()))
	log.Info().Msg(fmt.Sprintf("Auth Events: %d", authEventCount))
	log.Info().Msg(fmt.Sprintf("State Events: %d", stateEventCount))

	statsRoom := graph.Check(roomGraph)
	statsRoomTranspose := graph.Check(graph.Transpose(roomGraph))
	log.Info().Msg(fmt.Sprintf("Backward Extremities (Room DAG): %d", statsRoomTranspose.Isolated))
	if statsRoomTranspose.Isolated != 1 {
		log.Warn().Msg("There should only be one room event without parents!")
	}

	statsAuthChain := graph.Check(authChainGraph)
	statsAuthChainTranspose := graph.Check(graph.Transpose(authChainGraph))
	log.Info().Msg(fmt.Sprintf("Backward Extremities (Auth Chain): %d", statsAuthChainTranspose.Isolated))
	if statsAuthChainTranspose.Isolated != 1 {
		log.Warn().Msg("There should only be one auth chain event without parents!")
	}

	statsState := graph.Check(stateGraph)
	statsStateTranspose := graph.Check(graph.Transpose(stateGraph))
	log.Info().Msg(fmt.Sprintf("Backward Extremities (State DAG): %d", statsStateTranspose.Isolated))
	if statsStateTranspose.Isolated != 1 {
		log.Warn().Msg("There should only be one state event without parents!")
	}

	// TODO: Auth DAG

	log.Info().Msg(fmt.Sprintf("Forward Extremities (Room DAG): %d", statsRoom.Isolated))
	log.Info().Msg(fmt.Sprintf("Forward Extremities (Auth Chain): %d", statsAuthChain.Isolated))
	log.Info().Msg(fmt.Sprintf("Forward Extremities (State DAG): %d", statsState.Isolated))
	// TODO: Auth DAG

	log.Info().Msg(fmt.Sprintf("Room Edges: %d", statsRoom.Size))
	log.Info().Msg(fmt.Sprintf("Auth Chain Edges: %d", statsAuthChain.Size))
	log.Info().Msg(fmt.Sprintf("State Edges: %d", statsState.Size))
	// TODO: Auth DAG

	log.Info().Msg(fmt.Sprintf("Room DAG Child Count [# of children: # of nodes]: %v", roomChildCount))
	log.Info().Msg(fmt.Sprintf("Auth Chain Child Count [# of children: # of nodes]: %v", authChainChildCount))
	log.Info().Msg(fmt.Sprintf("State DAG Child Count [# of children: # of nodes]: %v", stateChildCount))
	// TODO: Auth DAG

	log.Info().Msg(fmt.Sprintf("Room DAG Size: %d, Max Depth: %d, Forks: %d", roomDAGSize, maxRoomDepth, roomDAGForks))
	log.Info().Msg(fmt.Sprintf("Auth Chain Size: %d, Max Depth: %d, Forks: %d", authChainSize, maxAuthChainDepth, authChainForks))
	log.Info().Msg(fmt.Sprintf("State DAG Size: %d, Max Depth: %d, Forks: %d", stateDAGSize, maxStateDepth, stateDAGForks))
	// TODO: Auth DAG

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
var stateDAGDepths = map[EventID]int{}

func traverseStateDAG(event *eventNode) {
	if _, ok := stateDAGSeenEvents[event.event.EventID]; !ok {
		stateDAGSize += 1
		if len(event.stateChildren) > 1 {
			stateDAGForks += 1
		}
	}

	stateDAGSeenEvents[event.event.EventID] = struct{}{}
	for childID, child := range event.stateChildren {
		if _, ok := stateDAGSeenEvents[childID]; !ok {
			traverseStateDAG(child)
		}

		stateDAGDepths[event.event.EventID] = int(math.Max(float64(stateDAGDepths[event.event.EventID]), float64(1+stateDAGDepths[childID])))
	}
}

var authChainSize = 0
var authChainForks = 0
var authChainSeenEvents = map[EventID]struct{}{}

// NOTE: must init all events to 0
var authChainDepths = map[EventID]int{}

func traverseAuthChain(event *eventNode) {
	if _, ok := authChainSeenEvents[event.event.EventID]; !ok {
		authChainSize += 1
		if len(event.authChainChildren) > 1 {
			authChainForks += 1
		}
	}

	authChainSeenEvents[event.event.EventID] = struct{}{}
	for childID, child := range event.authChainChildren {
		if _, ok := authChainSeenEvents[childID]; !ok {
			traverseAuthChain(child)
		}

		authChainDepths[event.event.EventID] = int(math.Max(float64(authChainDepths[event.event.EventID]), float64(1+authChainDepths[childID])))
	}
}

var roomDAGSize = 0
var roomDAGForks = 0
var roomDAGSeenEvents = map[EventID]struct{}{}

// NOTE: must init all events to 0
var roomDAGDepths = map[EventID]int{}

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

func (e *EventQueue) AddStateChild(eventID EventID, event *eventNode) {
	for _, queueEvent := range e.queue {
		if _, ok := queueEvent.stateChildren[eventID]; !ok {
			queueEvent.stateChildren[eventID] = event
		}
	}
}

func (e *EventQueue) AddStateChildrenFromNode(event *eventNode) {
	for _, queueEvent := range e.queue {
		for childID, child := range event.stateChildren {
			if _, ok := queueEvent.stateChildren[childID]; !ok {
				queueEvent.stateChildren[childID] = child
			}
		}
	}
}

var stateDAGCreationSeenEvents = map[EventID]struct{}{}

func createStateDAG(eventID EventID, event *eventNode, origin *eventNode, queue *EventQueue) {
	stateDAGCreationSeenEvents[eventID] = struct{}{}

	if event != origin && event.isStateEvent() {
		queue.AddStateChild(eventID, event)
		return
	}

	queue.Push(event)
	for childID, child := range event.roomChildren {
		// Only traverse children we haven't already seen
		if _, ok := stateDAGCreationSeenEvents[childID]; !ok {
			createStateDAG(childID, child, origin, queue)
		} else {
			if child.isStateEvent() {
				queue.AddStateChild(childID, child)
			} else {
				queue.AddStateChildrenFromNode(child)
			}
		}
	}
	queue.Pop()
}

func traverseRoomDAG(eventID EventID, event *eventNode) {
	if _, ok := roomDAGSeenEvents[eventID]; !ok {
		roomDAGSize += 1
		roomDAGSeenEvents[eventID] = struct{}{}
		if len(event.roomChildren) > 1 {
			roomDAGForks += 1
		}
	}

	for childID, child := range event.roomChildren {
		// Only traverse children we haven't already seen
		if _, ok := roomDAGSeenEvents[childID]; !ok {
			traverseRoomDAG(childID, child)
		}

		roomDAGDepths[eventID] = int(math.Max(float64(roomDAGDepths[eventID]), float64(1+roomDAGDepths[childID])))
	}
}
