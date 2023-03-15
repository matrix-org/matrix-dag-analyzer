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
	"sort"

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

// TODO: Create State DAG off Power DAG
// Will need to create a Power DAG mainline and link State events off it
// Find the latest power event in the each state event's auth chain
// Then create lists of State events per power event.
// Then can linearize state events off of that

// TODO: Create Timeline DAG off Power DAG
// Use the Power DAG mainline to link Timeline events off
// Find the latest power event in the each timeline event's auth chain
// Then create lists of timeline events per power event.
// Then can linearize timeline events off of that

// TODO: Create Timeline DAG off State DAG?
// Use the Power/State DAG mainline/s? to link Timeline events off

// TODO: Create function to linearize Timeline DAG

// TODO: Create AddEvent function which adds either a power/state/timeline event to the DAG

// TODO: Create an AuthEvent function which checks a power/state/timeline event if it's allowed?

// NOTE: All new events have a prev_power_event
// NOTE: New Power events only have a prev_power_event
// NOTE: New State events have a prev_state_event
// NOTE: New Timeline events have a prev_timeline_event

// NOTE: Can convert historical Room DAGs into new Power Event DAGs + State/Timeline DAGs
// NOTE: Should be backwards compatible, ie. servers not doing the new DAG stuff can still
// participate with conversion. But Room Versions should remove this problem anyway.
// NOTE: Power DAGs can be kept really small. All power DAG events need to be synced between servers.
// NOTE: State & Timeline DAGs don't rely on each other and both build off the Power DAG.
// NOTE: Might want to modify the member events to extract power events into their own type to
// make the logic & processing clearer & easier.

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

func ParseDAGFromFile(filename string, outputFilename string) (*RoomDAG, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	dag := NewRoomDAG()
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	signedJoins := 0
	joins := 0
	for scanner.Scan() {
		lineNumber = lineNumber + 1
		var event Event
		err := json.Unmarshal([]byte(scanner.Text()), &event)
		if err != nil {
			return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
		}

		switch event.Type {
		case EVENT_TYPE_CREATE:
			var content CreateEventContent
			err = json.Unmarshal(event.Content, &content)
			if err != nil {
				return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
			}
			event.CreateContent = &content
		case EVENT_TYPE_MEMBER:
			var content MemberEventContent
			err = json.Unmarshal(event.Content, &content)
			if err != nil {
				return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
			}
			joins++
			if content.JoinAuthorisedViaUsersServer != nil {
				signedJoins++
			}
			event.MembershipContent = &content
		}

		err = dag.addEvent(event)
		if err != nil {
			return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
		}
	}
	println("Joins: ", joins)
	println("Signed Joins: ", signedJoins)

	dag.roomMetrics = dag.generateDAGMetrics(RoomDAGType)
	dag.authChainMetrics = dag.generateDAGMetrics(AuthChainType)
	dag.generateDAG(AuthDAGType)
	dag.authMetrics = dag.generateDAGMetrics(AuthDAGType)
	dag.generateDAG(StateDAGType)
	dag.stateMetrics = dag.generateDAGMetrics(StateDAGType)
	dag.generateDAG(PowerDAGType)
	dag.powerMetrics = dag.generateDAGMetrics(PowerDAGType)

	// NOTE: To generate New State DAG
	// Traverse State DAG
	// Create New State DAG using isNewStateEvent()
	// Use that info to obtain the prev_state_events for each new state event
	// The prev_power_event for each new state event is obtained from the power event mainline
	// Use the same technique to generate prev_timeline_events & prev_power_event for each timeline event

	linearPowerDAG := dag.linearizePowerDAG()
	eventLine := []EventID{}
	for _, event := range linearPowerDAG {
		eventLine = append(eventLine, event.event.EventID)
	}
	log.Info().Msg(fmt.Sprintf("Linear Power DAG: %v", eventLine))
	newStateDAG := dag.linearizeStateDAG()
	log.Info().Msg(fmt.Sprintf("Size of Linear State DAG: %v", len(newStateDAG)))

	err = dag.generatePowerDAGJSON()
	if err != nil {
		return nil, err
	}

	return &dag, nil
}

func (d *RoomDAG) PrintPowerDAGJSON(outputFilename string) error {
	newEventsFile, err := os.Create(outputFilename)
	if err != nil {
		return err
	}

	for _, event := range d.eventsByID {
		if event.event != nil {
			if event.isPowerEvent() {
				experimentalEventJSON, err := json.Marshal(event.experimentalEvent)
				if err != nil {
					log.Err(err).Msg("Failed marshalling experimental event")
					return err
				}
				experimentalEventJSON = append(experimentalEventJSON, '\n')
				_, err = newEventsFile.Write(experimentalEventJSON)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (d *RoomDAG) generatePowerDAGJSON() error {
	// NOTE: Generate power DAG parent events
	for eventID, event := range d.eventsByID {
		if !event.isPowerEvent() {
			continue
		}
		for _, child := range event.powerChildren {
			child.powerParents[eventID] = event
		}
	}

	// NOTE: Generate new experimental events for each event
	for _, event := range d.eventsByID {
		if event.event != nil {
			oldEvent := event.event
			prevEvents := []string{}
			for parentID := range event.powerParents {
				prevEvents = append(prevEvents, parentID)
			}

			event.experimentalEvent = &ExperimentalEvent{
				EventID:     oldEvent.EventID,
				RoomVersion: oldEvent.RoomVersion,
				AuthEvents:  []string{}, // TODO: move the power parents here? or in powerEvents
				Content:     oldEvent.Content,
				Depth:       oldEvent.Depth, // TODO: what should this be now?
				Hashes:      oldEvent.Hashes,
				OriginTS:    oldEvent.OriginTS,
				PrevEvents:  prevEvents,
				Redacts:     oldEvent.Redacts,
				RoomID:      oldEvent.RoomID,
				Sender:      oldEvent.Sender,
				Signatures:  oldEvent.Signatures,
				StateKey:    oldEvent.StateKey,
				Type:        oldEvent.Type,
				Unsigned:    oldEvent.Unsigned,
			}
		}
	}

	// TODO: Stretch - add state & timeline events off the power DAG
	// Make a combined state & timeline DAG (excluding power DAG events)
	// How do I hang them off the power DAG?
	// How do I pick which power Event to reference?
	// Also, if we are syncing full power DAGs between servers, what if we don't
	// have the same power DAG as some other server?
	// We still need to get events from them. This should be minimized though since power DAGs
	// don't change that much or that fast.
	// Also, in the event of relaying, you can just send the full power DAG along with relay events.
	// This should always result in the events being able to be authed.

	// TODO:
	// The problem might arise where you can't auth previously unseen power DAG events against your current view of the room state.
	// In which case they may be discarded.
	// But unless the new events are relying on those to auth with, there should be no issue.
	// But what if they are?

	return nil
}

const DefaultPowerLevel = 0

func (d *RoomDAG) linearizePowerDAG() []*EventNode {
	// NOTE: The create event should always be first
	linearPowerDAG := []*EventNode{}
	incomingEdges := map[EventID]int{}
	outgoingEdges := map[EventID][]EventID{}
	for eventID, event := range d.eventsByID {
		if event.isPowerEvent() {
			// TODO: What is the difference in linearizing from top->bottom vs. bottom->top?
			// For one, any dangling backward extremities will be sorted differently
			// Also forward extremities are sorted differently

			// TODO: How do you obtain the current power level for a sender?
			// You would need to keep track of it changing over time for proper sorting.

			// TODO: Does this sorting make sense for power events?
			// ie. if bob's power level changes in a forward extremity, should that be considered
			// for top->bottom sorting, or bottom->top sorting? This could result in bob's power
			// level being different depending on the sorting algorithm...
			// Does this problem still in today's State DAG? All the extra state does is probably
			// make it less likely?

			incomingEdges[eventID] = len(event.powerChildren)
			for childID := range event.powerChildren {
				if _, ok := outgoingEdges[childID]; !ok {
					outgoingEdges[childID] = []EventID{}
				}
				outgoingEdges[childID] = append(outgoingEdges[childID], eventID)
			}
		}
	}

	// TODO: What I should actually do to maintain consistency:
	// Do kahn's algoritm backward as described (bottom->top)
	// At each step, if more than 1 event, postpone sorting until later
	// After I have a "sorted" list of lists, then go through from the start, applying
	// power levels as I go, and sort the remainder of the list
	// This keeps the semantics the same as they are now (maybe?) where we keep the forward extremities
	// at the end of the linearized DAG

	tempEventLine := [][]EventID{}

	nextEvents := []EventID{}
	for eventID, edgeCount := range incomingEdges {
		if edgeCount == 0 {
			nextEvents = append(nextEvents, eventID)
		}
	}

	for {
		if len(nextEvents) == 0 {
			break
		}

		tempEventLine = append(tempEventLine, nextEvents)

		for _, nextID := range nextEvents {
			for _, eventID := range outgoingEdges[nextID] {
				incomingEdges[eventID] -= 1
			}
			delete(incomingEdges, nextID)
		}

		nextEvents = []EventID{}
		for eventID, edgeCount := range incomingEdges {
			if edgeCount == 0 {
				nextEvents = append(nextEvents, eventID)
			}
		}
	}

	// NOTE: Reverse list so create event is first
	for i, j := 0, len(tempEventLine)-1; i < j; i, j = i+1, j-1 {
		tempEventLine[i], tempEventLine[j] = tempEventLine[j], tempEventLine[i]
	}

	// TODO: Sort sublists
	// TODO: How do we handle power levels if the create event refers to an older version of the room?

	// NOTE: From the create event until power levels are changed, the following rule applies:
	// "If the room contains no `m.room.power_levels` event, the room's creator has a power level of
	// 100, and all other users have a power level of 0."

	//creator := UserID(d.createEvent.event.CreateContent.Creator)
	//currentPowerLevels := map[UserID]PowerLevel{creator: 100}

	for i, events := range tempEventLine {
		if len(events) == 1 {
			linearPowerDAG = append(linearPowerDAG, d.eventsByID[events[0]])
			linearIndex := i
			d.eventsByID[events[0]].linearPowerIndex = &linearIndex
		} else {
			// TODO: Remove this after sorting sublists
			log.Error().Msg(fmt.Sprintf("Unsorted power events not being added to linear power DAG! Count: %d", len(events)))
		}
	}

	if linearPowerDAG[0] != d.createEvent {
		log.Panic().Msg(fmt.Sprintf("First event in linear power DAG (%s) is not create event (%s)", linearPowerDAG[0].event.EventID, d.createEvent.event.EventID))
	}

	return linearPowerDAG
}

type UserID string
type PowerLevel int

func (d *RoomDAG) sortPowerline(events []EventID, powerLevels map[UserID]PowerLevel) []EventID {
	if len(events) <= 1 {
		// Nothing to sort
		return events
	}

	orderedEvents := events

	sort.Slice(orderedEvents, func(i, j int) bool {
		eventI := d.eventsByID[orderedEvents[i]]
		eventJ := d.eventsByID[orderedEvents[j]]

		powerLevelI := PowerLevel(0)
		powerLevelJ := PowerLevel(0)
		if level, ok := powerLevels[UserID(eventI.event.EventID)]; ok {
			powerLevelI = level
		}
		if level, ok := powerLevels[UserID(eventJ.event.EventID)]; ok {
			powerLevelJ = level
		}

		if powerLevelI > powerLevelJ {
			return true
		} else if powerLevelI < powerLevelJ {
			return false
		}

		if eventI.event.OriginTS < eventJ.event.OriginTS {
			return true
		} else if eventI.event.OriginTS > eventJ.event.OriginTS {
			return false
		}

		if eventI.event.EventID < eventJ.event.EventID {
			return true
		} else {
			return false
		}
	})

	return orderedEvents
}

func (d *RoomDAG) calculateNewPowerLevels(events []EventID, powerLevels map[UserID]PowerLevel) map[UserID]PowerLevel {
	newPowerLevels := powerLevels

	// TODO: this
	//for _, eventID := range events {

	//}

	return newPowerLevels
}

func (d *RoomDAG) linearizeStateDAG() []*EventNode {
	// NOTE: This map contains info for the new Auth Chains of State Events
	mostRecentPowerEvent := map[EventID][]*EventNode{} // PowerEvent : []StateEvents
	linearizedStateDAG := []*EventNode{}               // []StateEvents
	for _, event := range d.eventsByID {
		if event.isNewStateEvent() {
			nextAuthEvents := []EventIDNode{}
			for parentID, parentEvent := range event.authChainParents {
				nextAuthEvents = append(nextAuthEvents, EventIDNode{parentID, parentEvent})
			}
			latestPowerEvent := d.findLatestPowerEvent(nextAuthEvents)
			if _, ok := mostRecentPowerEvent[latestPowerEvent.ID]; !ok {
				mostRecentPowerEvent[latestPowerEvent.ID] = []*EventNode{}
			}
			mostRecentPowerEvent[latestPowerEvent.ID] = append(mostRecentPowerEvent[latestPowerEvent.ID], event)
		}
	}

	// TODO: Sort linearized state sublists from mostRecentPowerEvent

	return linearizedStateDAG
}

type EventIDNode struct {
	ID   EventID
	Node *EventNode
}

func (d *RoomDAG) findLatestPowerEvent(authEvents []EventIDNode) EventIDNode {
	var latestPowerEvent EventIDNode
	latestIndex := 0
	found := false
	for _, authEvent := range authEvents {
		if index := d.eventsByID[authEvent.ID].linearPowerIndex; index != nil {
			found = true
			if *index > latestIndex {
				latestIndex = *index
				latestPowerEvent = authEvent
			}
		}
	}
	if !found {
		nextAuthEvents := []EventIDNode{}
		for _, authEvent := range authEvents {
			if authEvent.Node == nil {
				continue
			}

			for parentID, parentEvent := range authEvent.Node.authChainParents {
				nextAuthEvents = append(nextAuthEvents, EventIDNode{parentID, parentEvent})
			}
		}
		latestPowerEvent = d.findLatestPowerEvent(nextAuthEvents)
	}

	return latestPowerEvent
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
