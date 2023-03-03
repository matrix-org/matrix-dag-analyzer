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

// TODO: make globals go away
var newNodeIndex = 0

type eventNode struct {
	event      *Event
	roomIndex  int
	authIndex  *int
	stateIndex *int

	roomChildren map[EventID]*eventNode
	roomParents  map[EventID]*eventNode

	stateChildren     map[EventID]*eventNode
	authChildren      map[EventID]*eventNode
	authChainChildren map[EventID]*eventNode
	authChainParents  map[EventID]*eventNode
}

func newEventNode(event *Event, index int) eventNode {
	return eventNode{
		event:             event,
		roomIndex:         index,
		authIndex:         nil,
		stateIndex:        nil,
		roomChildren:      make(map[EventID]*eventNode),
		roomParents:       make(map[EventID]*eventNode),
		stateChildren:     make(map[EventID]*eventNode),
		authChildren:      make(map[EventID]*eventNode),
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
		newNodeIndex += 1
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
			newNodeIndex += 1
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
			newNodeIndex += 1
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

func GetGraphStats(input *graph.Mutable, graphType string) (graph.Stats, graph.Stats) {
	if !graph.Acyclic(input) {
		log.Error().Msg(graphType + " graph is not acyclic!")
		for _, cycle := range graph.StrongComponents(input) {
			log.Error().Msg(fmt.Sprintf("Found Cycle: %v", cycle))
		}
		panic(1)
	}

	statsRoom := graph.Check(input)
	statsRoomTranspose := graph.Check(graph.Transpose(input))
	if statsRoomTranspose.Isolated != 1 {
		log.Warn().Msg("There should only be one " + graphType + " event without parents!")
	}

	return statsRoom, statsRoomTranspose
}

func (d *RoomDAG) GenerateMetrics() {
	log.Info().Msg("***************************************************************")
	log.Info().Msg("Event Metrics:")
	for eventType, events := range d.eventsByType {
		log.Info().Msg(fmt.Sprintf("%s: %d", eventType, len(events)))
	}
	log.Info().Msg("***************************************************************")

	roomMetrics := d.GenerateDAGMetrics(RoomDAGType)
	statsRoom, statsRoomTranspose := GetGraphStats(roomMetrics.graph, "Room")

	authChainMetrics := d.GenerateDAGMetrics(AuthChainType)
	statsAuthChain, statsAuthChainTranspose := GetGraphStats(authChainMetrics.graph, "Auth Chain")

	d.GenerateDAG(AuthDAGType)
	authMetrics := d.GenerateDAGMetrics(AuthDAGType)
	statsAuth, statsAuthTranspose := GetGraphStats(authMetrics.graph, "Auth")

	d.GenerateDAG(StateDAGType)
	stateMetrics := d.GenerateDAGMetrics(StateDAGType)
	statsState, statsStateTranspose := GetGraphStats(stateMetrics.graph, "State")

	log.Info().Msg("***************************************************************")
	log.Info().Msg("DAG Metrics:")
	log.Info().Msg(fmt.Sprintf("Room Events: %d", d.TotalEvents()))
	log.Info().Msg(fmt.Sprintf("Auth Events: %d", authMetrics.size))
	log.Info().Msg(fmt.Sprintf("State Events: %d", stateMetrics.size))

	log.Info().Msg(fmt.Sprintf("Room DAG Edges: %d", statsRoom.Size))
	log.Info().Msg(fmt.Sprintf("Auth Chain Edges: %d", statsAuthChain.Size))
	log.Info().Msg(fmt.Sprintf("State DAG Edges: %d", statsState.Size))
	log.Info().Msg(fmt.Sprintf("Auth DAG Edges: %d", statsAuth.Size))

	log.Info().Msg(fmt.Sprintf("Backward Extremities (Room DAG): %d", statsRoomTranspose.Isolated))
	log.Info().Msg(fmt.Sprintf("Backward Extremities (Auth Chain): %d", statsAuthChainTranspose.Isolated))
	log.Info().Msg(fmt.Sprintf("Backward Extremities (State DAG): %d", statsStateTranspose.Isolated))
	log.Info().Msg(fmt.Sprintf("Backward Extremities (Auth DAG): %d", statsAuthTranspose.Isolated))

	log.Info().Msg(fmt.Sprintf("Forward Extremities (Room DAG): %d", statsRoom.Isolated))
	log.Info().Msg(fmt.Sprintf("Forward Extremities (Auth Chain): %d", statsAuthChain.Isolated))
	log.Info().Msg(fmt.Sprintf("Forward Extremities (State DAG): %d", statsState.Isolated))
	log.Info().Msg(fmt.Sprintf("Forward Extremities (Auth DAG): %d", statsAuth.Isolated))

	log.Info().Msg(fmt.Sprintf("Room DAG Child Count [# of children: # of nodes]: %v", roomMetrics.childCount))
	log.Info().Msg(fmt.Sprintf("Auth Chain Child Count [# of children: # of nodes]: %v", authChainMetrics.childCount))
	log.Info().Msg(fmt.Sprintf("State DAG Child Count [# of children: # of nodes]: %v", stateMetrics.childCount))
	log.Info().Msg(fmt.Sprintf("Auth DAG Child Count [# of children: # of nodes]: %v", authMetrics.childCount))

	log.Info().Msg(fmt.Sprintf("Room DAG Size: %d, Max Depth: %d, Forks: %d", roomMetrics.size, roomMetrics.maxDepth, roomMetrics.forks))
	log.Info().Msg(fmt.Sprintf("Auth Chain Size: %d, Max Depth: %d, Forks: %d", authChainMetrics.size, authChainMetrics.maxDepth, authChainMetrics.forks))
	log.Info().Msg(fmt.Sprintf("State DAG Size: %d, Max Depth: %d, Forks: %d", stateMetrics.size, stateMetrics.maxDepth, stateMetrics.forks))
	log.Info().Msg(fmt.Sprintf("Auth DAG Size: %d, Max Depth: %d, Forks: %d", authMetrics.size, authMetrics.maxDepth, authMetrics.forks))

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

type GraphMetrics struct {
	size       int
	forks      int
	seenEvents map[EventID]struct{}
	depths     map[EventID]int
	maxDepth   int
	childCount map[int]int
	graph      *graph.Mutable
}

type DAGTraversalMetrics struct {
	size       int
	forks      int
	seenEvents map[EventID]struct{}
	depths     map[EventID]int
}

type DAGGenerationMetrics struct {
	seenEvents map[EventID]struct{}
}

type DAGType int64

const (
	AuthDAGType DAGType = iota
	RoomDAGType
	StateDAGType
	AuthChainType
)

func GetEventTypeCheck(dagType DAGType) func(*eventNode) bool {
	switch dagType {
	case AuthDAGType:
		return func(node *eventNode) bool { return node.isAuthEvent() }
	case RoomDAGType:
		return func(node *eventNode) bool { return true }
	case StateDAGType:
		return func(node *eventNode) bool { return node.isStateEvent() }
	case AuthChainType:
		return func(node *eventNode) bool { return node.isAuthEvent() }
	default:
		panic(1)
	}
}

func (d *RoomDAG) GenerateDAG(dagType DAGType) {
	generationMetrics := DAGGenerationMetrics{
		seenEvents: map[EventID]struct{}{},
	}

	for nodeID, node := range d.eventsByID {
		queue := NewEventQueue()
		switch dagType {
		case AuthDAGType:
			generateAuthDAG(nodeID, node, node, &queue, &generationMetrics)
		case RoomDAGType:
		case StateDAGType:
			generateStateDAG(nodeID, node, node, &queue, &generationMetrics)
		case AuthChainType:
		}
	}
}

func (d *RoomDAG) GenerateDAGMetrics(dagType DAGType) GraphMetrics {
	traversalMetrics := DAGTraversalMetrics{
		size:       0,
		forks:      0,
		seenEvents: map[EventID]struct{}{},
		depths:     map[EventID]int{},
	}
	eventCount := 0
	maxDepth := 0
	childCount := map[int]int{}
	eventTypeCheck := GetEventTypeCheck(dagType)

	for nodeID, node := range d.eventsByID {
		if eventTypeCheck(node) {
			index := eventCount
			switch dagType {
			case AuthDAGType:
				node.authIndex = &index
			case StateDAGType:
				node.stateIndex = &index
			case AuthChainType:
				node.authIndex = &index
			}
			eventCount += 1
			traversalMetrics.depths[nodeID] = 0
		}
	}

	graph := graph.New(eventCount)
	for nodeID, node := range d.eventsByID {
		if eventTypeCheck(node) {
			switch dagType {
			case AuthDAGType:
				traverseAuthDAG(node, &traversalMetrics)
			case RoomDAGType:
				traverseRoomDAG(nodeID, node, &traversalMetrics)
			case StateDAGType:
				traverseStateDAG(node, &traversalMetrics)
			}
		}
	}

	// NOTE: Special since we only call it once
	if dagType == AuthChainType {
		traverseAuthChain(d.createEvent, &traversalMetrics)
	}

	for _, node := range d.eventsByID {
		if eventTypeCheck(node) {
			children := map[EventID]*eventNode{}
			var nodeIndex *int

			switch dagType {
			case AuthDAGType:
				children = node.authChildren
				nodeIndex = node.authIndex
			case RoomDAGType:
				children = node.roomChildren
				nodeIndex = &node.roomIndex
			case StateDAGType:
				children = node.stateChildren
				nodeIndex = node.stateIndex
			case AuthChainType:
				children = node.authChainChildren
				nodeIndex = node.authIndex
			}

			for _, child := range children {
				var childIndex *int
				switch dagType {
				case AuthDAGType:
					childIndex = child.authIndex
				case RoomDAGType:
					childIndex = &child.roomIndex
				case StateDAGType:
					childIndex = child.stateIndex
				case AuthChainType:
					childIndex = child.authIndex
				}
				graph.Add(*nodeIndex, *childIndex)
			}

			count, ok := childCount[len(children)]
			if !ok {
				childCount[len(children)] = 1
			} else {
				childCount[len(children)] = count + 1
			}
		}
	}

	for _, depth := range traversalMetrics.depths {
		maxDepth = int(math.Max(float64(maxDepth), float64(depth)))
	}

	return GraphMetrics{
		size:       traversalMetrics.size,
		forks:      traversalMetrics.forks,
		seenEvents: traversalMetrics.seenEvents,
		depths:     traversalMetrics.depths,
		maxDepth:   maxDepth,
		childCount: childCount,
		graph:      graph,
	}
}

// TODO: Refactor these common functions
func traverseRoomDAG(eventID EventID, event *eventNode, metrics *DAGTraversalMetrics) {
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

func traverseAuthChain(event *eventNode, metrics *DAGTraversalMetrics) {
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

func traverseStateDAG(event *eventNode, metrics *DAGTraversalMetrics) {
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

func traverseAuthDAG(event *eventNode, metrics *DAGTraversalMetrics) {
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

// TODO: Refactor these common functions
func generateStateDAG(eventID EventID, event *eventNode, origin *eventNode, queue *EventQueue, metrics *DAGGenerationMetrics) {
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

func generateAuthDAG(eventID EventID, event *eventNode, origin *eventNode, queue *EventQueue, metrics *DAGGenerationMetrics) {
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
