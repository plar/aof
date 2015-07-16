package aof

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLexerTokens(t *testing.T) {
	tests := []struct {
		data   string
		tokens []tokenType
	}{
		{data: "", tokens: []tokenType{tokenEOF}},
		{data: "   \t   ", tokens: []tokenType{tokenSpace, tokenEOF}},
		{data: "\n\r\n\r\n\r", tokens: []tokenType{tokenEOL, tokenEOL, tokenEOL, tokenEOF}},
		{data: "\n\n\n", tokens: []tokenType{tokenEOL, tokenEOL, tokenEOL, tokenEOF}},
		{data: "0", tokens: []tokenType{tokenNumber, tokenEOF}},
		{data: "0\n", tokens: []tokenType{tokenNumber, tokenEOL, tokenEOF}},
		{data: "0\nkey1   1234\n", tokens: []tokenType{tokenNumber, tokenEOL, tokenString, tokenSpace, tokenNumber, tokenEOL, tokenEOF}},
		{data: "0\nkey1\t1234\n", tokens: []tokenType{tokenNumber, tokenEOL, tokenString, tokenSpace, tokenNumber, tokenEOL, tokenEOF}},
		{data: `2
key1 1
key2    2
CREATE key1 1
CREATE  key2 3
MODIFY key2   +   4
            `, tokens: []tokenType{
			tokenNumber, tokenEOL,
			tokenString, tokenSpace, tokenNumber, tokenEOL,
			tokenString, tokenSpace, tokenNumber, tokenEOL,
			tokenString, tokenSpace, tokenString, tokenSpace, tokenNumber, tokenEOL,
			tokenString, tokenSpace, tokenString, tokenSpace, tokenNumber, tokenEOL,
			tokenString, tokenSpace, tokenString, tokenSpace, tokenString, tokenSpace, tokenNumber, tokenEOL,
			tokenSpace, tokenEOF}},
	}

	for i, test := range tests {
		func() {
			quit := make(chan struct{})
			defer close(quit)

			l := newLexer(quit, strings.NewReader(test.data))
			go l.run()

			tokens := []tokenType{}
			for {
				token := l.nextToken()
				tokens = append(tokens, token.typ)

				if token.typ == tokenError || token.typ == tokenEOF {
					break
				}
			}

			if !reflect.DeepEqual(test.tokens, tokens) {
				assert.Fail(t, fmt.Sprintf("%d) Expected %v != Actual %v", i, test.tokens, tokens))
			}
		}()
	}
}

func TestLexerWithValues(t *testing.T) {
	tests := []struct {
		data   string
		tokens []token
		err    error
	}{
		{data: `2
key1 1
key2 2
CREATE key1 1
CREATE key2 3
MODIFY key2 +4
`, tokens: []token{
			{typ: tokenNumber, val: "2"}, {typ: tokenEOL},
			{typ: tokenString, val: "key1"}, {typ: tokenSpace}, {typ: tokenNumber, val: "1"}, {typ: tokenEOL},
			{typ: tokenString, val: "key2"}, {typ: tokenSpace}, {typ: tokenNumber, val: "2"}, {typ: tokenEOL},
			{typ: tokenString, val: "CREATE"}, {typ: tokenSpace}, {typ: tokenString, val: "key1"}, {typ: tokenSpace}, {typ: tokenNumber, val: "1"}, {typ: tokenEOL},
			{typ: tokenString, val: "CREATE"}, {typ: tokenSpace}, {typ: tokenString, val: "key2"}, {typ: tokenSpace}, {typ: tokenNumber, val: "3"}, {typ: tokenEOL},
			{typ: tokenString, val: "MODIFY"}, {typ: tokenSpace}, {typ: tokenString, val: "key2"}, {typ: tokenSpace}, {typ: tokenNumber, val: "+4"}, {typ: tokenEOL},
			{typ: tokenEOF}}, err: nil},
	}

	for i, test := range tests {
		func() {
			quit := make(chan struct{})
			l := newLexer(quit, strings.NewReader(test.data))
			go l.run()
			defer close(quit)

			tokens := []token{}
			for {
				token := l.nextToken()
				tokens = append(tokens, token)
				if token.typ == tokenError || token.typ == tokenEOF {
					break
				}
			}

			if !reflect.DeepEqual(test.tokens, tokens) {
				assert.Fail(t, fmt.Sprintf("%d) Not matched\nExpected: %v\n  Actual: %v", i, test.tokens, tokens))
			}
		}()
	}
}

type fakeReader struct {
	sync.WaitGroup
}

func (fr *fakeReader) Read(p []byte) (n int, err error) {
	time.Sleep(2 * time.Second)
	fr.Done()
	return 1, nil
}

func TestLexerQuit(t *testing.T) {
	fr := &fakeReader{}
	fr.Add(1)

	quit := make(chan struct{})
	l := newLexer(quit, fr)
	go l.run()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		l.nextToken()
		fr.Wait()
		wg.Done()
	}()

	go func() {
		close(quit)
	}()
	wg.Wait()
}
