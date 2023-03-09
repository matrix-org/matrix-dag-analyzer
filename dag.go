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

type GraphMetrics struct {
	size       int
	forks      int
	seenEvents map[EventID]struct{}
	depths     map[EventID]int
	maxDepth   int
	childCount map[int]int
	graph      *graph.Mutable
}

// TODO: Create Power DAG
// How? Just extract the power events from the State DAG
// What are the power events?
// m.room.create
// m.room.power_levels
// m.room.join_rules
// m.room.member -> membership == leave || ban, sender != state_key
// m.room.server_acl? I think this might be necessary as well
// Start by unmarshalling the membership events so I can parse them

// TODO: Create function to linearize Power DAG

// TODO: Create State DAG off Power DAG
// Will need to create a Power DAG mainline and link State events off it

// TODO: Create Timeline DAG off Power DAG
// Use the Power DAG mainline to link Timeline events off

// TODO: Create Timeline DAG off State DAG?
// Use the Power/State DAG mainline/s? to link Timeline events off

// TODO: Create function to linearize Timeline DAG

// TODO: Create AddEvent function which adds either a power/state/timeline event to the DAG

// TODO: Create an AuthEvent function which checks a power/state/timeline event if it's allowed?

type RoomDAG struct {
	eventCount int

	eventsByID   map[EventID]*EventNode
	eventsByType map[EventType][]*EventNode

	createEvent *EventNode
	roomID      *string

	roomMetrics      GraphMetrics
	authChainMetrics GraphMetrics
	stateMetrics     GraphMetrics
	authMetrics      GraphMetrics

	powerMetrics GraphMetrics
}

func NewRoomDAG() RoomDAG {
	return RoomDAG{
		eventCount:   0,
		eventsByID:   make(map[EventID]*EventNode),
		eventsByType: make(map[EventType][]*EventNode),
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

		if event.Type == EVENT_TYPE_MEMBER {
			var content MemberEventContent
			err = json.Unmarshal(event.Content, &content)
			if err != nil {
				return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
			}
			event.MembershipContent = &content
		}

		err = dag.addEvent(event)
		if err != nil {
			return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
		}
	}

	dag.roomMetrics = dag.generateDAGMetrics(RoomDAGType)
	dag.authChainMetrics = dag.generateDAGMetrics(AuthChainType)
	dag.generateDAG(AuthDAGType)
	dag.authMetrics = dag.generateDAGMetrics(AuthDAGType)
	dag.generateDAG(StateDAGType)
	dag.stateMetrics = dag.generateDAGMetrics(StateDAGType)

	dag.generateDAG(PowerDAGType)
	dag.powerMetrics = dag.generateDAGMetrics(PowerDAGType)

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

func getGraphStats(input *graph.Mutable, graphType string) (graph.Stats, graph.Stats) {
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

func (d *RoomDAG) PrintMetrics() {
	log.Info().Msg("***************************************************************")

	log.Info().Msg("Event Metrics:")
	for eventType, events := range d.eventsByType {
		log.Info().Msg(fmt.Sprintf("%s: %d", eventType, len(events)))
	}

	log.Info().Msg("***************************************************************")

	statsRoom, statsRoomTranspose := getGraphStats(d.roomMetrics.graph, "Room")
	statsAuthChain, statsAuthChainTranspose := getGraphStats(d.authChainMetrics.graph, "Auth Chain")
	statsAuth, statsAuthTranspose := getGraphStats(d.authMetrics.graph, "Auth")
	statsState, statsStateTranspose := getGraphStats(d.stateMetrics.graph, "State")

	log.Info().Msg("***************************************************************")

	log.Info().Msg("DAG Metrics:")
	log.Info().Msg(fmt.Sprintf("Room Events: %d", d.TotalEvents()))
	log.Info().Msg(fmt.Sprintf("Auth Events: %d", d.authMetrics.size))
	log.Info().Msg(fmt.Sprintf("State Events: %d", d.stateMetrics.size))

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

	log.Info().Msg(fmt.Sprintf("Room DAG Child Count [# of children: # of nodes]: %v", d.roomMetrics.childCount))
	log.Info().Msg(fmt.Sprintf("Auth Chain Child Count [# of children: # of nodes]: %v", d.authChainMetrics.childCount))
	log.Info().Msg(fmt.Sprintf("State DAG Child Count [# of children: # of nodes]: %v", d.stateMetrics.childCount))
	log.Info().Msg(fmt.Sprintf("Auth DAG Child Count [# of children: # of nodes]: %v", d.authMetrics.childCount))

	log.Info().Msg(fmt.Sprintf("Room DAG Size: %d, Max Depth: %d, Forks: %d", d.roomMetrics.size, d.roomMetrics.maxDepth, d.roomMetrics.forks))
	log.Info().Msg(fmt.Sprintf("Auth Chain Size: %d, Max Depth: %d, Forks: %d", d.authChainMetrics.size, d.authChainMetrics.maxDepth, d.authChainMetrics.forks))
	log.Info().Msg(fmt.Sprintf("State DAG Size: %d, Max Depth: %d, Forks: %d", d.stateMetrics.size, d.stateMetrics.maxDepth, d.stateMetrics.forks))
	log.Info().Msg(fmt.Sprintf("Auth DAG Size: %d, Max Depth: %d, Forks: %d", d.authMetrics.size, d.authMetrics.maxDepth, d.authMetrics.forks))

	log.Info().Msg("***************************************************************")

	log.Info().Msg(fmt.Sprintf("Power DAG: => m.room.{create, power_levels, join_rules, member.ban, member.leave}"))
	statsPower, statsPowerTranspose := getGraphStats(d.powerMetrics.graph, "Power")
	log.Info().Msg(fmt.Sprintf("Power DAG Edges: %d", statsPower.Size))
	log.Info().Msg(fmt.Sprintf("Backward Extremities (Power DAG): %d", statsPowerTranspose.Isolated))
	log.Info().Msg(fmt.Sprintf("Forward Extremities (Power DAG): %d", statsPower.Isolated))
	log.Info().Msg(fmt.Sprintf("Power DAG Child Count [# of children: # of nodes]: %v", d.powerMetrics.childCount))
	log.Info().Msg(fmt.Sprintf("Power DAG Size: %d, Max Depth: %d, Forks: %d", d.powerMetrics.size, d.powerMetrics.maxDepth, d.powerMetrics.forks))

	log.Info().Msg("***************************************************************")
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
		newNode := newEventNode(&newEvent, d.eventCount)
		d.eventCount += 1
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
		d.eventsByType[newEvent.Type] = []*EventNode{newNode}
	}

	for _, authEventID := range newEvent.AuthEvents {
		if _, ok := d.eventsByID[authEventID]; !ok {
			// NOTE: add a placeholder event
			newNode := newEventNode(nil, d.eventCount)
			d.eventCount += 1
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
			newNode := newEventNode(nil, d.eventCount)
			d.eventCount += 1
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
	PowerDAGType
)

func getEventTypeCheck(dagType DAGType) func(*EventNode) bool {
	switch dagType {
	case AuthDAGType:
		return func(node *EventNode) bool { return node.isAuthEvent() }
	case RoomDAGType:
		return func(node *EventNode) bool { return true }
	case StateDAGType:
		return func(node *EventNode) bool { return node.isStateEvent() }
	case AuthChainType:
		return func(node *EventNode) bool { return node.isAuthEvent() }
	case PowerDAGType:
		return func(node *EventNode) bool { return node.isPowerEvent() }
	default:
		panic(1)
	}
}

func (d *RoomDAG) generateDAG(dagType DAGType) {
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
		case PowerDAGType:
			generatePowerDAG(nodeID, node, node, &queue, &generationMetrics)
		}
	}
}

func (d *RoomDAG) generateDAGMetrics(dagType DAGType) GraphMetrics {
	traversalMetrics := DAGTraversalMetrics{
		size:       0,
		forks:      0,
		seenEvents: map[EventID]struct{}{},
		depths:     map[EventID]int{},
	}
	eventCount := 0
	maxDepth := 0
	childCount := map[int]int{}
	eventTypeCheck := getEventTypeCheck(dagType)

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
			case PowerDAGType:
				node.powerIndex = &index
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
			case PowerDAGType:
				traversePowerDAG(node, &traversalMetrics)
			}
		}
	}

	// NOTE: Special since we only call it once
	if dagType == AuthChainType {
		traverseAuthChain(d.createEvent, &traversalMetrics)
	}

	for _, node := range d.eventsByID {
		if eventTypeCheck(node) {
			children := map[EventID]*EventNode{}
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
			case PowerDAGType:
				children = node.powerChildren
				nodeIndex = node.powerIndex
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
				case PowerDAGType:
					childIndex = child.powerIndex
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
