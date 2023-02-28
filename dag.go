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
	"errors"
	"fmt"
	"os"
)

type eventNode struct {
	event         *Event
	stateChildren map[EventID]*Event
	authChildren  map[EventID]*Event
}

func newEventNode(event *Event) eventNode {
	return eventNode{
		event:         event,
		stateChildren: make(map[EventID]*Event),
		authChildren:  make(map[EventID]*Event),
	}
}

type RoomDAG struct {
	eventsByID   map[EventID]*Event
	eventsByType map[EventType][]*Event

	createEvent *eventNode
	roomID      *string
}

func NewRoomDAG() RoomDAG {
	return RoomDAG{
		eventsByID:   make(map[EventID]*Event),
		eventsByType: make(map[EventType][]*Event),
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
	for scanner.Scan() {
		var event Event
		err := json.Unmarshal([]byte(scanner.Text()), &event)
		if err != nil {
			return nil, err
		}

		err = dag.addEvent(event)
		if err != nil {
			return nil, err
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

func (d *RoomDAG) addEvent(event Event) error {
	if _, ok := d.eventsByID[event.EventID]; ok {
		return errors.New("Duplicate event ID detected in file")
	}
	if d.roomID != nil && *d.roomID != event.RoomID {
		return fmt.Errorf("Received event with different room ID. Expected: %s, Got: %s", *d.roomID, event.RoomID)
	}
	if d.roomID == nil {
		d.roomID = &event.RoomID
	}

	d.eventsByID[event.EventID] = &event

	if events, ok := d.eventsByType[event.Type]; ok {
		d.eventsByType[event.Type] = append(events, &event)
	} else {
		d.eventsByType[event.Type] = []*Event{&event}
	}

	// TODO: make DAG

	return nil
}
