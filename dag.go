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
	"time"

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

// TODO: Create an AuthEvent function which checks whether a power/state/timeline event is allowed
// TODO: Create function to linearize State/Timeline DAG
// TODO: Create AddEvent function which adds either a power/state/timeline event to the DAG
// TODO: Are power events still part of the total set of state events? I don't think there is reason
// to overlap them anymore.

// NOTE: All new events have a prev_power_event
// NOTE: New Power events only have a prev_power_event
// NOTE: New State events also have a prev_event
// NOTE: New Timeline events also have a prev_event

// NOTE: Can convert historical Room DAGs into new Power Event DAGs + State/Timeline DAGs
// NOTE: Should be backwards compatible, ie. servers not doing the new DAG stuff can still
// participate with conversion. But Room Versions should remove this problem anyway.
// NOTE: Power DAGs can be kept really small. All power DAG events need to be synced between servers.
// NOTE: State & Timeline DAGs don't rely on each other and both build off the Power DAG.
// NOTE: Might want to modify the member events to extract power events into their own type to
// make the logic & processing clearer & easier.

type AuthResult int64

const (
	AuthReject AuthResult = iota
	AuthSoftFail
	AuthAccept
)

type LinearPowerDAG []*EventNode

// NOTE: The powerDAG input is the entirety of the linearized power DAG
func EventAuthExperimental(event *EventNode, powerDAG LinearPowerDAG) AuthResult {
	// TODO: This
	// Check against the power event it points to
	// Also check against the current state of the power DAG (ie. latest power state)

	// NOTE: Comments with # specify the section of the spec's auth rules

	switch event.event.Type {
	case EVENT_TYPE_CREATE:
		// 1.
		if len(event.newPrevEvents) > 0 {
			return AuthReject
		}
		// TODO: This: Same as current matrix rules
		// if room:domain != sender:domain { reject }
		// if content.room_version is present & not recognised { reject }
		// if content.creator isn't present { reject }
		return AuthAccept
	case EVENT_TYPE_MEMBER:
		// 4.
		// TODO: This: Same as current matrix rules
	case EVENT_TYPE_THIRD_PARTY_INVITE:
		// 6.
		// if sender power level < invite { reject }
	case EVENT_TYPE_POWER_LEVELS:
		// 9.
		// TODO: This: Same as current matrix rules
	}

	// 2.
	// TODO: This: Same as current matrix rules
	// if prev_events.power_event != event.power_event { reject }
	// if event.power_event is rejected { reject }
	// if event.power_event isn't in power DAG { reject }

	// 3.
	// TODO: This: Same as current matrix rules
	// if m.room.create.content.federate == false && sender:domain != create.sender:domain { reject }

	// 5.
	// if sender membership state is not join { reject }
	// NOTE: instead of "is join / is not join" - check "is allowed / is not allowed"
	// This is really the magic that makes auth different than current matrix
	// Biggest problem is with the invite/leave sequencing on invite-only rooms.
	// ie. is there an invite for this user, have they left since that invite, is there a new invite
	// after the leave, what if there are multiple invites out before a leave (do they all cancel out), etc.
	// Start by sorting out bans (What about kicks? are they any different than bans?)
	// Then invite/leave chains
	// Then look further into restricted rooms

	// TODO: The following logic for both event power event & current room state
	eventRoomState := roomStateAtEvent(event, powerDAG)
	currentRoomState := roomStateAtEvent(powerDAG[len(powerDAG)-1], powerDAG)
	isSenderAllowed := func(roomState RoomState, sender UserID) bool {
		isAllowed := false
		if roomState.isBannedUser(sender) {
			return false
		}

		if roomState.isInviteOnlyRoom() {
			isAllowed = roomState.hasOutstandingInvite(sender)
		} else if roomState.isRestrictedRoom() {
			isAllowed = roomState.hasOutstandingInvite(sender) || roomState.hasSignedJoin(sender)
		} else {
			isAllowed = true
		}

		return isAllowed
	}
	if !isSenderAllowed(eventRoomState, UserID(event.event.Sender)) {
		return AuthReject
	}
	if !isSenderAllowed(currentRoomState, UserID(event.event.Sender)) {
		return AuthSoftFail
	}

	// 7.
	// TODO: This: Same as current matrix rules
	// if type's power level > sender's power level { reject }

	// 8.
	// TODO: This: Same as current matrix rules
	// if event.state_key starts with "@" & != sender { reject }

	return AuthAccept
}

type RoomMode int64

const (
	RoomModePublic RoomMode = iota
	RoomModeInvite
	RoomModeRestricted
)

type RoomState struct {
	Mode RoomMode
}

func roomStateAtEvent(event *EventNode, powerDAG LinearPowerDAG) RoomState {
	// TODO: This
	// Return linearized room state culmination from this event
	return RoomState{}
}

func (r *RoomState) isInviteOnlyRoom() bool {
	return r.Mode == RoomModeInvite
}

func (r *RoomState) isRestrictedRoom() bool {
	return r.Mode == RoomModeRestricted
}

func (r *RoomState) isPublicRoom() bool {
	return r.Mode == RoomModePublic
}

func (r *RoomState) isBannedUser(sender UserID) bool {
	// TODO: This
	// Look at bans & ACLs?
	return true
}

func (r *RoomState) hasOutstandingInvite(sender UserID) bool {
	// Look at invite map
	// TODO: This
	return false
}

func (r *RoomState) hasSignedJoin(sender UserID) bool {
	// Look at signed joins map
	// TODO: This
	return false
}

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

	linearizedPowerDAG LinearPowerDAG
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
	leaves := 0
	bans := 0
	invites := 0
	leavesByOtherMember := 0
	for scanner.Scan() {
		lineNumber = lineNumber + 1
		var event Event
		err := json.Unmarshal([]byte(scanner.Text()), &event)
		if err != nil {
			return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
		}

		if dag.IsDuplicate(event) {
			continue
		}

		switch event.Type {
		case EVENT_TYPE_CREATE:
			var content CreateEventContent
			err = json.Unmarshal(event.Content, &content)
			if err != nil {
				log.Err(fmt.Errorf("Line %d: %w", lineNumber, err)).Msg("Skipping malformed event")
				continue
			}
			event.CreateContent = &content
		case EVENT_TYPE_MEMBER:
			var content MemberEventContent
			err = json.Unmarshal(event.Content, &content)
			if err != nil {
				log.Err(fmt.Errorf("Line %d: %w", lineNumber, err)).Msg("Skipping malformed event")
				continue
			}
			if content.Membership == "join" {
				joins++
			} else if content.Membership == "leave" {
				leaves++
				if event.StateKey != nil && event.Sender != *event.StateKey {
					leavesByOtherMember++
				}
			} else if content.Membership == "ban" {
				bans++
			} else if content.Membership == "invite" {
				invites++
			}
			if content.JoinAuthorisedViaUsersServer != nil {
				signedJoins++
			}
			event.MembershipContent = &content
		case EVENT_TYPE_POWER_LEVELS:
			var content PowerLevelsEventContent
			err = json.Unmarshal(event.Content, &content)
			if err != nil {
				log.Err(fmt.Errorf("Line %d: %w", lineNumber, err)).Msg("Skipping malformed event")
				continue
			}
			event.PowerLevelsContent = &content
		}

		err = dag.addEvent(event)
		if err != nil {
			return nil, fmt.Errorf("Line %d: %w", lineNumber, err)
		}
	}
	log.Info().Msg(fmt.Sprintf("Member Leaves (total): %d", leaves))
	log.Info().Msg(fmt.Sprintf("Member Leaves (by other member): %d", leavesByOtherMember))
	log.Info().Msg(fmt.Sprintf("Member Bans: %d", bans))
	log.Info().Msg(fmt.Sprintf("Member Invites: %d", invites))
	log.Info().Msg(fmt.Sprintf("Member Joins: %d", joins))
	log.Info().Msg(fmt.Sprintf("Member Signed Joins: %d", signedJoins))

	log.Info().Msg("Generating Room DAG")
	dag.roomMetrics = dag.generateDAGMetrics(RoomDAGType)
	dag.authChainMetrics = dag.generateDAGMetrics(AuthChainType)
	log.Info().Msg("Generating Auth DAG")
	dag.generateDAG(AuthDAGType)
	dag.authMetrics = dag.generateDAGMetrics(AuthDAGType)
	log.Info().Msg("Generating State DAG")
	dag.generateDAG(StateDAGType)
	dag.stateMetrics = dag.generateDAGMetrics(StateDAGType)
	log.Info().Msg("Generating Power DAG")
	dag.generateDAG(PowerDAGType)
	dag.powerMetrics = dag.generateDAGMetrics(PowerDAGType)
	err = dag.generateExperimentalEvents()
	if err != nil {
		return nil, err
	}

	log.Info().Msg("Linearizing Power DAG")
	dag.createLinearizedPowerDAG()
	dag.linearizeStateAndTimelineDAG()

	return &dag, nil
}

func (d *RoomDAG) CreatePowerDAGJSON(outputFilename string) error {
	newEventsFile, err := os.Create(outputFilename)
	if err != nil {
		return err
	}

	for _, event := range d.eventsByID {
		if event.event != nil {
			//if event.isPowerEvent() {
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
			//}
		}
	}

	return nil
}

func (d *RoomDAG) generateExperimentalEvents() error {
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
				AuthEvents:  []string{}, // TODO: move the power parents here? or in PowerEvents
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
	// How do I pick which power Event to reference? Latest linearized power event from their auth chain.
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

func (d *RoomDAG) createLinearizedPowerDAG() {
	// NOTE: The create event should always be first
	linearPowerDAG := LinearPowerDAG{}
	incomingEdgeCounts := map[*EventNode]int{}
	for _, event := range d.eventsByID {
		if event.isPowerEvent() {
			// NOTE: TLDR: Sorting power events top->bottom seems like the way to go

			// TODO: What is the difference in linearizing from top->bottom vs. bottom->top?
			// For one, any dangling backward extremities will be sorted differently
			// Also forward extremities are sorted differently
			// Dangling backward extremities seem like they shouldn't be valid anyway. Where do they even come from?

			// TODO: How do you obtain the current power level for a sender?
			// You would need to keep track of it changing over time for proper sorting.

			// TODO: Does this sorting make sense for power events?
			// ie. if bob's power level changes in a forward extremity, should that be considered
			// for top->bottom sorting, or bottom->top sorting? This could result in bob's power
			// level being different depending on the sorting algorithm...
			// Does this problem still in today's State DAG? All the extra state does is probably
			// make it less likely?
			// This seems very similar to State Resets in state res v2: https://github.com/matrix-org/internal-config/issues/844
			for _, child := range event.powerChildren {
				if _, ok := incomingEdgeCounts[child]; !ok {
					incomingEdgeCounts[child] = 0
				}
				incomingEdgeCounts[child]++
			}
		}
	}

	// NOTE: Linearization algorithm used:
	// Sort using kahn's algorithm from top->bottom (create->extremities)
	// At each step, if more than 1 event, postpone sorting until later
	// After I have a "sorted" list of lists, then go through from the start, applying
	// power levels as I go, and sort the remainder of the list

	tempEventLine := [][]*EventNode{}
	nextEvents := []*EventNode{d.createEvent}
	for {
		if len(nextEvents) == 0 {
			break
		}

		tempEventLine = append(tempEventLine, nextEvents)

		for _, next := range nextEvents {
			for _, powerChild := range next.powerChildren {
				incomingEdgeCounts[powerChild]--
			}
			delete(incomingEdgeCounts, next)
		}

		nextEvents = []*EventNode{}
		for eventID, edgeCount := range incomingEdgeCounts {
			if edgeCount == 0 {
				nextEvents = append(nextEvents, eventID)
			}
		}
	}

	// TODO: How do we handle power levels if the create event refers to an older version of the room?
	// NOTE: From the create event until power levels are changed, the following rule applies:
	// "If the room contains no `m.room.power_levels` event, the room's creator has a power level of
	// 100, and all other users have a power level of 0."

	currentPowerLevels := PowerLevels{Default: 0, Users: map[UserID]PowerLevel{}}
	for i, events := range tempEventLine {
		sortedEvents := d.sortPowerline(events, currentPowerLevels)
		for _, event := range sortedEvents {
			currentPowerLevels = d.calculateNewPowerLevels(event, currentPowerLevels)
			linearPowerDAG = append(linearPowerDAG, event)
			linearIndex := i
			event.linearPowerIndex = &linearIndex
		}
	}

	if linearPowerDAG[0] != d.createEvent {
		log.Panic().Msg(fmt.Sprintf("First event in linear power DAG (%s) is not create event (%s)", linearPowerDAG[0].event.EventID, d.createEvent.event.EventID))
	}

	d.linearizedPowerDAG = linearPowerDAG
}

type UserID string
type PowerLevel int

func (d *RoomDAG) sortPowerline(events []*EventNode, powerLevels PowerLevels) LinearPowerDAG {
	if len(events) <= 1 {
		// Nothing to sort
		return events
	}

	orderedEvents := events
	sort.Slice(orderedEvents, func(i, j int) bool {
		eventI := orderedEvents[i]
		eventJ := orderedEvents[j]

		powerLevelI := PowerLevel(powerLevels.Default)
		powerLevelJ := PowerLevel(powerLevels.Default)
		if level, ok := powerLevels.Users[UserID(eventI.event.EventID)]; ok {
			powerLevelI = level
		}
		if level, ok := powerLevels.Users[UserID(eventJ.event.EventID)]; ok {
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

type PowerLevels struct {
	Default PowerLevel
	Users   map[UserID]PowerLevel
}

func (d *RoomDAG) calculateNewPowerLevels(event *EventNode, powerLevels PowerLevels) PowerLevels {
	newPowerLevels := powerLevels

	switch event.event.Type {
	case EVENT_TYPE_CREATE:
		newPowerLevels.Default = 0
		newPowerLevels.Users = map[UserID]PowerLevel{}
		newPowerLevels.Users[UserID(event.event.CreateContent.Creator)] = 100
	case EVENT_TYPE_POWER_LEVELS:
		// Upon receiving one of these events, room creator no longer has PL = 100 unless specified by this event
		newPowerLevels.Default = PowerLevel(event.event.PowerLevelsContent.UsersDefault)
		newPowerLevels.Users = map[UserID]PowerLevel{}
		for user, powerLevel := range event.event.PowerLevelsContent.Users {
			newPowerLevels.Users[UserID(user)] = PowerLevel(powerLevel)
		}
	default:
		// TODO: can anything else change power levels?
	}

	return newPowerLevels
}

func (d *RoomDAG) linearizeStateAndTimelineDAG() []*EventNode {
	log.Info().Msg("Calculating latest power event for each room event...")
	// NOTE: This map contains info for the new Auth Chains of State Events
	mostRecentPowerEvent := map[EventID]map[*EventNode]struct{}{} // PowerEvent : []Event
	linearizedDAG := []*EventNode{}
	for _, event := range d.eventsByID {
		if event.isTimelineOrStateEvent() {
			nextEvents := []*EventIDNode{}
			//for parentID, parentEvent := range event.roomParents {
			for parentID, parentEvent := range event.authChainParents {
				nextEvents = append(nextEvents, &EventIDNode{parentID, parentEvent})
			}
			latestPowerEvent := d.findLatestPowerEvent(nextEvents)
			if latestPowerEvent == nil {
				log.Warn().
					Str("ID", event.event.EventID).
					Str("Type", event.event.Type).
					Msg("This event's auth chain doesn't link up to a power event...")
				for parentID := range event.authChainParents {
					log.Warn().Msg(fmt.Sprintf("Parent: %s", parentID))
					log.Warn().Msg(fmt.Sprintf("Create: %s", d.createEvent.event.EventID))
				}
			}
			if latestPowerEvent != nil {
				if _, ok := mostRecentPowerEvent[latestPowerEvent.ID]; !ok {
					mostRecentPowerEvent[latestPowerEvent.ID] = map[*EventNode]struct{}{}
				}
				mostRecentPowerEvent[latestPowerEvent.ID][event] = struct{}{}
				event.experimentalEvent.AuthEvents = []string{latestPowerEvent.ID}
			}
		}
	}

	// TODO: Set the prev_events for non-power nodes
	// what are they?
	// They would be the same prev_events as before possibly?
	// The prev events need to have the same power event reference
	// Should they refer to all forward extremity power events? not just the one latest in the linear timeline?
	// Only new power events should consolidate power event extremities.
	// State & timeline events should choose that power event that is the latest in the timeline base on
	// linearization rules.
	// Is this right and/or a good idea???
	// So, ideally, all state/timeline events should have prev_events that are on the same branch (same power event), and
	// the prev_events try to consolidate all forward extremities on that branch

	// TODO: What if they choose prev_events that aren't on the same branch?
	// This should only happen if the server sending that event isn't following the rules, which means it should be rejected.
	// Do you auth against the prev_events for this case?
	// This requires obtaining the prev_events if you don't have them already.
	// Something like... If all prev_events specify the same power_event as this event, then accept. Otherwise reject.
	// What if you can't obtain one or more of the prev_events? Maybe accept it as long as all you can obtain are on the same power_event?

	// TODO: What do we do with events that don't have any prev events that contain a matching power event?

	for _, powerEvent := range d.linearizedPowerDAG {
		if events, ok := mostRecentPowerEvent[powerEvent.event.EventID]; ok {
			for event := range events {
				event.newPrevPowerEvent = powerEvent

				// TODO: Remove!!!
				//exists := false
				//for _, prevEvent := range event.experimentalEvent.PrevEvents {
				//	if prevEvent == powerEvent.event.EventID {
				//		exists = true
				//		break
				//	}
				//}
				//if !exists {
				//	event.experimentalEvent.PrevEvents = append(event.experimentalEvent.PrevEvents, powerEvent.event.EventID)
				//}
			}
			// TODO: Sort event sublist & append to linearizedDAG
		}
	}

	for _, powerEvent := range d.linearizedPowerDAG {
		seenEvents := map[EventID]struct{}{}
		queue := NewEventQueue()
		d.findBranchChildren(powerEvent.event.EventID, powerEvent, powerEvent, powerEvent, &queue, seenEvents)
		for childID, child := range powerEvent.tempChildren {
			powerEvent.newRoomChildren[childID] = child
		}

		if events, ok := mostRecentPowerEvent[powerEvent.event.EventID]; ok {
			for event := range events {
				queue = NewEventQueue()
				d.findBranchChildren(event.event.EventID, event, event, powerEvent, &queue, seenEvents)
				for childID, child := range event.tempChildren {
					event.newRoomChildren[childID] = child
				}
			}
		}

		// Clear the temp maps
		for _, event := range d.eventsByID {
			event.tempChildren = map[string]*EventNode{}
		}
	}

	for eventID, event := range d.eventsByID {
		for _, child := range event.newRoomChildren {
			child.newPrevEvents[eventID] = event

			// TODO: Remove!!!
			exists := false
			for _, prevEvent := range child.experimentalEvent.PrevEvents {
				if prevEvent == eventID {
					exists = true
					break
				}
			}
			if !exists {
				child.experimentalEvent.PrevEvents = append(child.experimentalEvent.PrevEvents, eventID)
			}
		}
	}

	zeroCount := 0
	branchCount := 0
	eventCount := 0
	for _, powerEvent := range d.linearizedPowerDAG {
		if events, ok := mostRecentPowerEvent[powerEvent.event.EventID]; ok {
			branchCount++
			for event := range events {
				eventCount++
				if len(event.newPrevEvents) == 0 {
					zeroCount++
					log.Warn().Msg(fmt.Sprintf("No prev events with matching power event: ID: %s Timestamp: %v Sender: %s Type: %s", event.event.EventID, time.Unix(event.event.OriginTS/1000, 0), event.event.Sender, event.event.Type))
				}
				for _, prev := range event.newPrevEvents {
					if prev.newPrevPowerEvent != powerEvent && prev != powerEvent {
						log.Panic().Msg(fmt.Sprintf("Uh oh! Prev: %v, Power: %s", prev.newPrevPowerEvent.event.EventID, powerEvent.event.EventID))
					}
				}
			}
		}
	}
	log.Info().Msg(fmt.Sprintf("Tiered DAG: eventCount: %d branchCount: %d zeroCount: %d", eventCount, branchCount, zeroCount))

	return linearizedDAG
}

func (d *RoomDAG) findBranchChildren(eventID EventID, event *EventNode, origin *EventNode, powerEvent *EventNode, queue *EventQueue, seenEvents map[EventID]struct{}) {
	seenEvents[eventID] = struct{}{}

	if event != origin && event.newPrevPowerEvent == powerEvent {
		queue.AddChild(eventID, event, NewStateTimelineEvent)
		return
	}

	queue.Push(event)
	for childID, child := range event.roomChildren {
		if _, ok := seenEvents[childID]; !ok {
			d.findBranchChildren(childID, child, origin, powerEvent, queue, seenEvents)
		} else {
			if child.newPrevPowerEvent == powerEvent {
				queue.AddChild(childID, child, NewStateTimelineEvent)
			} else {
				queue.AddChildrenFromNode(child, NewStateTimelineEvent)
			}
		}
	}
	queue.Pop()

	return
}

type EventIDNode struct {
	ID   EventID
	Node *EventNode
}

func (d *RoomDAG) findLatestPowerEvent(nextEvents []*EventIDNode) *EventIDNode {
	if len(nextEvents) == 0 {
		return nil
	}

	var latestPowerEvent *EventIDNode
	latestIndex := -1
	for _, nextEvent := range nextEvents {
		if index := d.eventsByID[nextEvent.ID].linearPowerIndex; index != nil {
			if *index > latestIndex {
				latestIndex = *index
				latestPowerEvent = nextEvent
			}
		}
	}

	newNextEvents := []*EventIDNode{}
	for _, nextEvent := range nextEvents {
		if nextEvent.Node == nil {
			continue
		}

		// NOTE : Unsure whether to use auth parents or room parents to select latest power event
		//for parentID, parent := range nextEvent.Node.roomParents {
		for parentID, parent := range nextEvent.Node.authChainParents {
			newNextEvents = append(newNextEvents, &EventIDNode{parentID, parent})
		}
	}
	if len(newNextEvents) > 0 {
		newEvent := d.findLatestPowerEvent(newNextEvents)
		if index := d.eventsByID[newEvent.ID].linearPowerIndex; index != nil {
			if *index > latestIndex {
				latestIndex = *index
				latestPowerEvent = newEvent
			}
		}
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

	log.Info().Msg(fmt.Sprintf("Power DAG: => m.room.{create, power_levels, join_rules, third_party_invite, server_acl, member.ban, member.invite, member.leave, member.restricted_join}"))
	statsPower, statsPowerTranspose := getGraphStats(d.powerMetrics.graph, "Power")
	log.Info().Msg(fmt.Sprintf("Power DAG Edges: %d", statsPower.Size))
	log.Info().Msg(fmt.Sprintf("Backward Extremities (Power DAG): %d", statsPowerTranspose.Isolated))
	log.Info().Msg(fmt.Sprintf("Forward Extremities (Power DAG): %d", statsPower.Isolated))
	log.Info().Msg(fmt.Sprintf("Power DAG Child Count [# of children: # of nodes]: %v", d.powerMetrics.childCount))
	log.Info().Msg(fmt.Sprintf("Power DAG Size: %d, Max Depth: %d, Forks: %d", d.powerMetrics.size, d.powerMetrics.maxDepth, d.powerMetrics.forks))

	log.Info().Msg("***************************************************************")
}

func (d *RoomDAG) IsDuplicate(newEvent Event) bool {
	if foundEvent, ok := d.eventsByID[newEvent.EventID]; ok && foundEvent.event != nil {
		return true
	}
	return false
}

func (d *RoomDAG) addEvent(newEvent Event) error {
	if d.IsDuplicate(newEvent) {
		//log.Warn().Msg(fmt.Sprintf("Duplicate EventID detected: %v", newEvent))
		return nil
		//return fmt.Errorf("Duplicate event ID detected in file: %s", newEvent.EventID)
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
