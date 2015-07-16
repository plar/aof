package aof

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type EventType int

const (
	EventError EventType = 1 << iota
	EventHeader
	EventCreate
	EventDelete
	EventModify
	EventSet
	EventCompleted
	EventQuit
	EventFinal
)

var events2str = map[EventType]string{
	EventError:     "EventError",
	EventHeader:    "EventHeader",
	EventCreate:    "EventCreate",
	EventDelete:    "EventDelete",
	EventModify:    "EventModify",
	EventSet:       "EventSet",
	EventCompleted: "EventCompleted",
	EventQuit:      "EventQuit",
	EventFinal:     "EventFinal",
}

type Event struct {
	Type    EventType
	Key     string
	Value   int
	Deleted bool
}

func (e Event) String() string {
	var isFinalEvent = ""
	if (e.Type & EventFinal) == EventFinal {
		isFinalEvent = "|EventFinal"
	}

	typ := (e.Type & ^EventFinal)
	evnt, exists := events2str[EventType(typ)]
	if !exists {
		return fmt.Sprintf("Unknown event: %v", e.Type)
	}

	return fmt.Sprintf("Event{Type: %v%v, Key: %s, Value: %d, Deleted: %v}", evnt, isFinalEvent, e.Key, e.Value, e.Deleted)
}

type parserStateFunc func(*AOFParser) parserStateFunc

type value struct {
	val     int
	deleted bool
}

type AOFParser struct {
	quit   chan struct{}
	lex    *lexer
	events chan Event
	state  parserStateFunc
	err    error

	headerTotal   int
	headers       map[string]int
	values        map[string]value
	curHeaderLine int
	curBodyLine   int
	lastValidLine int

	curEvent EventType
	curKey   string
	curDelta int
	curValue int
}

func NewAOFParser(rd io.Reader) *AOFParser {
	quit := make(chan struct{})
	return &AOFParser{
		quit:    quit,
		lex:     newLexer(quit, rd),
		events:  make(chan Event),
		headers: make(map[string]int),
		values:  make(map[string]value),
	}
}

func newEvent(typ EventType) Event {
	return Event{Type: typ}
}

func (p *AOFParser) error(format string, args ...interface{}) {
	prefix := fmt.Sprintf("ERROR at line %d: ", p.curHeaderLine+p.curBodyLine+1) // lines start from 1
	p.err = errors.New(prefix + fmt.Sprintf(format, args...))
	p.emit(Event{Type: EventError, Key: p.curKey, Value: p.curValue, Deleted: p.values[p.curKey].deleted})
}

func (p *AOFParser) nextNonSpace() (t token) {
	for {
		t = p.next()
		if t.typ != tokenSpace {
			break
		}
	}
	return t
}

func (p *AOFParser) expect(typ tokenType) token {
	t := p.nextNonSpace()
	if t.typ != typ {
		p.unexpected(t.typ, typ)
	}
	return t
}

func (p *AOFParser) expectOneOf(expectedValues ...tokenType) token {
	t := p.nextNonSpace()
	for _, typ := range expectedValues {
		if t.typ == typ {
			return t
		}
	}

	p.unexpected(t.typ, expectedValues...)
	return t
}

func (p *AOFParser) unexpected(actual tokenType, expected ...tokenType) {
	if len(expected) == 1 {
		p.error("Unexpected token: %v, expected %v", actual, expected[0])
	} else {
		p.error("Unexpected token: %v, expected one of %v", actual, expected)
	}
}

func aofHeaderTotal(p *AOFParser) parserStateFunc {
	token := p.expect(tokenNumber)
	if token.typ != tokenNumber {
		return nil
	}

	i64, _ := strconv.ParseInt(token.val, 10, 64)
	p.headerTotal = int(i64)
	if p.headerTotal <= 0 {
		p.emit(newEvent(EventCompleted))
		return nil
	}

	p.expect(tokenEOL)

	p.curHeaderLine++

	return aofHeader
}

func aofHeader(p *AOFParser) parserStateFunc {
	for i := 0; i < p.headerTotal; i++ {
		key := p.expectOneOf(tokenString, tokenNumber)
		if key.typ != tokenString && key.typ != tokenNumber {
			return nil
		}

		rawLastLine := p.expect(tokenNumber)
		if rawLastLine.typ != tokenNumber {
			return nil
		}

		lastLine, _ := strconv.ParseInt(rawLastLine.val, 10, 32)
		p.headers[key.val] = int(lastLine)

		if int(lastLine) > p.lastValidLine {
			p.lastValidLine = int(lastLine)
		}

		p.expect(tokenEOL)

		p.curHeaderLine++
	}

	p.emit(newEvent(EventHeader))

	return aofBodyEvent
}

func aofBodyEvent(p *AOFParser) parserStateFunc {
	rawEvent := p.expect(tokenString)
	if rawEvent.typ != tokenString {
		return nil
	}

	action := strings.ToUpper(rawEvent.val)
	switch action {
	case "CREATE":
		p.curEvent = EventCreate
	case "DELETE":
		p.curEvent = EventDelete
	case "MODIFY":
		p.curEvent = EventModify
	case "SET":
		p.curEvent = EventSet
	default:
		p.error("Unknown action: %s", rawEvent.val)
		return nil
	}

	return aofBodyKey
}

func aofBodyKey(p *AOFParser) parserStateFunc {
	rawKey := p.expectOneOf(tokenString, tokenNumber)
	if rawKey.typ != tokenString && rawKey.typ != tokenNumber {
		return nil
	}
	p.curKey = rawKey.val

	if p.curEvent == EventCreate || p.curEvent == EventSet {
		return aofBodyValue
	} else if p.curEvent == EventModify {
		return aofBodyModifyOperator
	} else {
		return aofEmitBodyEvent
	}
}

func aofBodyValue(p *AOFParser) parserStateFunc {
	rawValue := p.expect(tokenNumber)
	if rawValue.typ != tokenNumber {
		return nil
	}

	v64, _ := strconv.ParseInt(rawValue.val, 10, 64)
	p.curValue = int(v64)

	return aofEmitBodyEvent
}

func aofBodyModifyOperator(p *AOFParser) parserStateFunc {
	rawDelta := p.expect(tokenNumber)
	if rawDelta.typ != tokenNumber {
		return nil
	}

	if !strings.HasPrefix(rawDelta.val, "+") && !strings.HasPrefix(rawDelta.val, "-") {
		p.error("Unknown MODIFY operator: %s", rawDelta.val)
		return nil
	}

	v64, _ := strconv.ParseInt(rawDelta.val, 10, 64)
	p.curDelta = int(v64)

	return aofEmitBodyEvent
}

func aofEmitBodyEvent(p *AOFParser) parserStateFunc {

	if _, exists := p.headers[p.curKey]; !exists {
		p.error("Key '%s' was not defined in the header", p.curKey)
		return nil
	}

	switch p.curEvent {
	case EventCreate:
		v, exists := p.values[p.curKey]
		if exists && !v.deleted {
			p.error("Key '%s' has already been created", p.curKey)
			return nil
		}

		p.values[p.curKey] = value{val: p.curValue, deleted: false}

	case EventSet:
		_, exists := p.values[p.curKey]
		if !exists {
			p.error("Key '%s' was not created", p.curKey)
			return nil
		}

		p.values[p.curKey] = value{val: p.curValue, deleted: false}

	case EventModify:
		v, exists := p.values[p.curKey]
		if !exists {
			p.error("Key '%s' was not created", p.curKey)
			return nil
		}

		v.val = v.val + p.curDelta
		p.values[p.curKey] = v

	case EventDelete:
		v, exists := p.values[p.curKey]
		if !exists {
			p.error("Key '%s' was not created", p.curKey)
			return nil
		} else if v.deleted {
			p.error("Key '%s' has been deleted", p.curKey)
			return nil
		}

		v.deleted = true
		p.values[p.curKey] = v
	}

	var eventType = p.curEvent
	if p.headers[p.curKey] == p.curBodyLine {
		eventType |= EventFinal
	}

	p.emit(Event{Type: eventType, Key: p.curKey, Value: p.values[p.curKey].val, Deleted: p.values[p.curKey].deleted})

	return aofBodyNextLine
}

func aofBodyNextLine(p *AOFParser) parserStateFunc {
	p.curBodyLine++
	if p.curBodyLine <= p.lastValidLine {
		t := p.expectOneOf(tokenEOL, tokenEOF)
		if t.typ == tokenEOF && p.curBodyLine < p.lastValidLine {
			p.error("Unexpected EOF, expected at least %d line(s)", p.lastValidLine-p.curBodyLine+1)
			return nil
		} else if t.typ == tokenEOL {
			return aofBodyEvent
		}
	}

	p.emit(newEvent(EventCompleted))
	return nil
}

func (p *AOFParser) emit(event Event) {
	select {
	case p.events <- event:
	case <-p.quit:
		close(p.events)
	}
}

func (p *AOFParser) next() token {
	return p.lex.nextToken()
}

func (p *AOFParser) NextEvent() Event {
	select {
	case event := <-p.events:
		return event
	case <-p.quit:
		close(p.events)
		return newEvent(EventQuit)
	}
}

func (p *AOFParser) Quit() {
	close(p.quit)
}

func (p *AOFParser) Error() error {
	return p.err
}

func (p *AOFParser) Parse() {
	go p.lex.run()

	for p.state = aofHeaderTotal; p.state != nil; {
		p.state = p.state(p)
	}
}
