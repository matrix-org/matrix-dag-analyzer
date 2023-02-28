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

const EVENT_TYPE_CREATE = "m.room.create"
const EVENT_TYPE_JOIN_RULE = "m.room.join_rule"
const EVENT_TYPE_MEMBER = "m.room.member"
const EVENT_TYPE_POWER_LEVELS = "m.room.power_levels"
const EVENT_TYPE_THIRD_PARTY_INVITE = "m.room.third_party_invite"

type EventID = string

type EventType = string

type Event struct {
	EventID    string      `json:"event_id"`
	AuthEvents interface{} `json:"auth_events"`
	Content    RawJSON     `json:"content"`
	Depth      int64       `json:"depth"`
	Hashes     RawJSON     `json:"hashes"`
	OriginTS   int64       `json:"origin_server_ts"`
	PrevEvents interface{} `json:"prev_events"`
	Redacts    *string     `json:"redacts,omitempty"`
	RoomID     string      `json:"room_id"`
	Sender     string      `json:"sender"`
	Signatures RawJSON     `json:"signatures,omitempty"`
	StateKey   *string     `json:"state_key,omitempty"`
	Type       string      `json:"type"`
	Unsigned   RawJSON     `json:"unsigned,omitempty"`
}

type RawJSON []byte

// MarshalJSON implements the json.Marshaller interface using a value receiver.
// This means that RawJSON used as an embedded value will still encode correctly.
func (r RawJSON) MarshalJSON() ([]byte, error) {
	return []byte(r), nil
}

// UnmarshalJSON implements the json.Unmarshaller interface using a pointer receiver.
func (r *RawJSON) UnmarshalJSON(data []byte) error {
	*r = RawJSON(data)
	return nil
}
