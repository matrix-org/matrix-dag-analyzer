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
const EVENT_TYPE_JOIN_RULES = "m.room.join_rules"
const EVENT_TYPE_MEMBER = "m.room.member"
const EVENT_TYPE_POWER_LEVELS = "m.room.power_levels"
const EVENT_TYPE_THIRD_PARTY_INVITE = "m.room.third_party_invite"
const EVENT_TYPE_SERVER_ACL = "m.room.server_acl"

var AuthEventTypes = map[string]struct{}{
	EVENT_TYPE_CREATE:             {},
	EVENT_TYPE_JOIN_RULES:         {},
	EVENT_TYPE_MEMBER:             {},
	EVENT_TYPE_POWER_LEVELS:       {},
	EVENT_TYPE_THIRD_PARTY_INVITE: {},
}

func IsAuthEvent(eventType EventType) bool {
	_, ok := AuthEventTypes[eventType]
	return ok
}

func IsPowerEvent(event *Event) bool {
	isPowerEventType := event.Type == EVENT_TYPE_CREATE ||
		event.Type == EVENT_TYPE_POWER_LEVELS ||
		event.Type == EVENT_TYPE_JOIN_RULES ||
		event.Type == EVENT_TYPE_THIRD_PARTY_INVITE ||
		event.Type == EVENT_TYPE_SERVER_ACL ||
		event.Type == EVENT_TYPE_MEMBER
	memberIsPower := false
	if event.Type == EVENT_TYPE_MEMBER {
		if content := event.MembershipContent; content != nil && event.StateKey != nil {
			if content.Membership == "ban" ||
				content.Membership == "invite" ||
				(content.Membership == "join" && content.JoinAuthorisedViaUsersServer != nil) ||
				// TODO: Shouldn't we include all "leave" events???
				(content.Membership == "leave" && event.Sender != *event.StateKey) {
				memberIsPower = true
			}
		}
	} else {
		memberIsPower = true
	}

	return isPowerEventType && memberIsPower
}

type EventID = string

type EventType = string

type Event struct {
	EventID            string   `json:"_event_id"`
	RoomVersion        string   `json:"_room_version"`
	AuthEvents         []string `json:"auth_events"`
	Content            RawJSON  `json:"content"`
	Depth              int64    `json:"depth"`
	Hashes             RawJSON  `json:"hashes"`
	OriginTS           int64    `json:"origin_server_ts"`
	PrevEvents         []string `json:"prev_events"`
	Redacts            *string  `json:"redacts,omitempty"`
	RoomID             string   `json:"room_id"`
	Sender             string   `json:"sender"`
	Signatures         RawJSON  `json:"signatures,omitempty"`
	StateKey           *string  `json:"state_key,omitempty"`
	Type               string   `json:"type"`
	Unsigned           RawJSON  `json:"unsigned,omitempty"`
	MembershipContent  *MemberEventContent
	CreateContent      *CreateEventContent
	PowerLevelsContent *PowerLevelsEventContent
}

type ExperimentalEvent struct {
	EventID     string   `json:"_event_id"`
	RoomVersion string   `json:"_room_version"`
	AuthEvents  []string `json:"auth_events"`
	Content     RawJSON  `json:"content"`
	Depth       int64    `json:"depth"`
	Hashes      RawJSON  `json:"hashes"`
	OriginTS    int64    `json:"origin_server_ts"`
	PrevEvents  []string `json:"prev_events"`
	Redacts     *string  `json:"redacts,omitempty"`
	RoomID      string   `json:"room_id"`
	Sender      string   `json:"sender"`
	Signatures  RawJSON  `json:"signatures,omitempty"`
	StateKey    *string  `json:"state_key,omitempty"`
	Type        string   `json:"type"`
	Unsigned    RawJSON  `json:"unsigned,omitempty"`
}

type MemberEventContent struct {
	Membership                   string   `json:"membership"`
	AvatarURL                    *string  `json:"avatar_url,omitempty"`
	DisplayName                  *string  `json:"displayname,omitempty"`
	IsDirect                     *bool    `json:"is_direct,omitempty"`
	JoinAuthorisedViaUsersServer *string  `json:"join_authorised_via_users_server,omitempty"`
	Reason                       *string  `json:"reason,omitempty"`
	ThirdPartyInvite             *RawJSON `json:"third_party_invite,omitempty"`
}

type PreviousRoom struct {
	EventID string `json:"event_id"`
	RoomID  string `json:"room_id"`
}

type CreateEventContent struct {
	Creator     string       `json:"creator"`
	Federate    *bool        `json:"m.federate,omitempty"`
	Predecessor PreviousRoom `json:"predecessor,omitempty"`
	RoomVersion string       `json:"room_version,omitempty"`
	Type        string       `json:"type,omitempty"`
}

type PowerLevelsEventContent struct {
	Users        map[string]int `json:"users"`
	UsersDefault int            `json:"users_default"`
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
