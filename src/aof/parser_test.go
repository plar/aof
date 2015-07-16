package aof

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func eventsEqual(e1 []Event, e2 []Event, checkBody bool) bool {
	if len(e1) != len(e2) {
		return false
	}

	for i := 0; i < len(e1); i++ {
		if e1[i].Type != e2[i].Type {
			return false
		}

		if checkBody {
			if e1[i].Type == EventHeader {
				continue
			}

			if e1[i].Key != e2[i].Key || e1[i].Value != e2[i].Value || e1[i].Deleted != e2[i].Deleted {
				//fmt.Printf("%v -- %v\n", e1[i], e2[i])
				return false
			}
		}
	}

	return true
}

func TestParser(t *testing.T) {

	tests := []struct {
		aof        string
		events     []Event
		checkBody  bool
		checkError bool
		err        string
	}{
		{
			aof: `0`,
			events: []Event{
				Event{Type: EventCompleted},
			},
			checkBody: false,
		},
		{
			aof: ``,
			events: []Event{
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 1: Unexpected token: tokenEOF, expected tokenNumber",
		},
		{
			aof: `
			`,
			events: []Event{
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 1: Unexpected token: tokenEOL, expected tokenNumber",
		},
		{
			aof: `1

`,
			events: []Event{
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 2: Unexpected token: tokenEOL, expected one of [tokenString tokenNumber]",
		},
		{
			aof: `1
12345
`,
			events: []Event{
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 2: Unexpected token: tokenEOL, expected tokenNumber",
		},
		{
			aof: `1
key1 ABC
`,
			events: []Event{
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 2: Unexpected token: tokenString, expected tokenNumber",
		},
		{
			aof: `1
key1 0
ACTION X Y Z
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 3: Unknown action: ACTION",
		},
		{
			aof: `1
key1 1
CREATE KeyUnknown 1000
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 3: Key 'KeyUnknown' was not defined in the header",
		},
		{
			aof: `1
1234 0
CREATE 1234 1000
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate | EventFinal, Key: "1234", Value: 1000},
				Event{Type: EventCompleted},
			},
			checkError: true,
			err:        "",
		},
		{
			aof: `1
1234 1
CREATE 1234 1000

`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate},
				Event{Type: EventError},
			},
			checkBody:  false,
			checkError: true,
			err:        "ERROR at line 4: Unexpected token: tokenEOL, expected tokenString",
		},
		{
			aof: `1
key1 0

CREATE key1 1000
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 3: Unexpected token: tokenEOL, expected tokenString",
		},
		{
			aof: `1
key1 0
CREATE key1 VALUE
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 3: Unexpected token: tokenString, expected tokenNumber",
		},
		{
			aof: `1
key1 1
CREATE key1 1
SET key1 VALUE
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate},
				Event{Type: EventError},
			},
			checkBody:  false,
			checkError: true,
			err:        "ERROR at line 4: Unexpected token: tokenString, expected tokenNumber",
		},

		{
			aof: `1
key1 1
CREATE key1 1
MODIFY key1 +VALUE
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate},
				Event{Type: EventError},
			},
			checkBody:  false,
			checkError: true,
			err:        "ERROR at line 4: Unexpected token: tokenString, expected tokenNumber",
		},

		{
			aof: `1
keyX 1
CREATE keyX 2
CREATE keyX 3
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate, Key: "keyX", Value: 2},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 4: Key 'keyX' has already been created",
		},

		{
			aof: `1
keyX 0
MODIFY keyX 2
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 3: Unknown MODIFY operator: 2",
		},

		{
			aof: `1
keyX 0
CREATE
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 3: Unexpected token: tokenEOL, expected one of [tokenString tokenNumber]",
		},

		{
			aof: `1
keyX 10
CREATE keyX 1`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 4: Unexpected EOF, expected at least 10 line(s)",
		},

		{
			aof: `1
keyX 0
MODIFY keyX +2
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 3: Key 'keyX' was not created",
		},
		{
			aof: `1
keyX 0
DELETE keyX
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 3: Key 'keyX' was not created",
		},
		{
			aof: `1
keyX 0
SET keyX 2
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 3: Key 'keyX' was not created",
		},
		{
			aof: `1
keyX 2
CREATE keyX 1
DELETE keyX
CREATE keyX 1000
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate, Key: "keyX", Value: 1},
				Event{Type: EventDelete, Key: "keyX", Value: 1, Deleted: true},
				Event{Type: EventCreate | EventFinal, Key: "keyX", Value: 1000, Deleted: false},
				Event{Type: EventCompleted},
			},
			checkBody: true,
		},
		{
			aof: `1
keyX 2
CREATE keyX 1
DELETE keyX
MODIFY keyX +1
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate},
				Event{Type: EventDelete},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 5: Key 'keyX' was not created",
		},
		{
			aof: `1
keyX 2
CREATE keyX 1
DELETE keyX
SET keyX +1
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate},
				Event{Type: EventDelete},
				Event{Type: EventError},
			},
			checkError: true,
			err:        "ERROR at line 5: Key 'keyX' was not created",
		},
		{
			aof: `1
keyX 2
CREATE keyX 1
DELETE keyX
DELETE keyX
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate, Key: "keyX", Value: 1},
				Event{Type: EventDelete, Key: "keyX", Value: 1, Deleted: true},
				Event{Type: EventError, Key: "keyX", Value: 1, Deleted: true},
			},
			checkBody:  true,
			checkError: true,
			err:        "ERROR at line 5: Key 'keyX' has been deleted",
		},
		{
			aof: `5
key1 1
key2 2
key3 3
key4 6
key5 10
CREATE  key1 1000
MODIFY  key1 +1
CREATE  key2 2000
CREATE  key3 3000
CREATE  key4 4000
SET     key4 4500
DELETE  key4
CREATE key5 5000
MODIFY  key5 +1
MODIFY  key5 +1
MODIFY  key5 -1
`,
			events: []Event{
				Event{Type: EventHeader},
				Event{Type: EventCreate, Key: "key1", Value: 1000},
				Event{Type: EventModify | EventFinal, Key: "key1", Value: 1001},
				Event{Type: EventCreate | EventFinal, Key: "key2", Value: 2000},
				Event{Type: EventCreate | EventFinal, Key: "key3", Value: 3000},
				Event{Type: EventCreate, Key: "key4", Value: 4000},
				Event{Type: EventSet, Key: "key4", Value: 4500},
				Event{Type: EventDelete | EventFinal, Key: "key4", Value: 4500, Deleted: true},
				Event{Type: EventCreate, Key: "key5", Value: 5000},
				Event{Type: EventModify, Key: "key5", Value: 5001},
				Event{Type: EventModify, Key: "key5", Value: 5002},
				Event{Type: EventModify | EventFinal, Key: "key5", Value: 5001},
				Event{Type: EventCompleted},
			},
			checkBody: true,
		},
	}

	for i, test := range tests {
		func() {
			//fmt.Println("Test ", i)
			aof := NewAOFParser(strings.NewReader(test.aof))
			go aof.Parse()
			defer aof.Quit()

			events := []Event{}
			for {
				event := aof.NextEvent()
				events = append(events, event)
				if event.Type == EventQuit || event.Type == EventError || event.Type == EventCompleted {
					if event.Type == EventError {
						if test.checkError {
							assert.Equal(t, test.err, aof.err.Error())
						} else {
							assert.Fail(t, aof.err.Error())
						}

					}
					break
				}
			}

			if !eventsEqual(test.events, events, test.checkBody) {
				assert.Fail(t, fmt.Sprintf("%d) Events are mismatched\nExpected: %v\n  Actual: %v\n", i, test.events, events))
			}
		}()
	}

}
