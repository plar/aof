package aof

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
)

type tokenType int

const (
	tokenError tokenType = iota
	tokenSpace
	tokenNumber
	tokenString
	tokenEOL
	tokenEOF
	tokenQuit
)

func (tt tokenType) String() string {
	switch tt {
	case tokenError:
		return "tokenError"
	case tokenSpace:
		return "tokenSpace"
	case tokenNumber:
		return "tokenNumber"
	case tokenString:
		return "tokenString"
	case tokenEOL:
		return "tokenEOL"
	case tokenEOF:
		return "tokenEOF"
	default:
		return "tokenUnknow"
	}

}

type token struct {
	typ tokenType
	val string
}

type stateFunc func(*lexer) stateFunc

type lexer struct {
	rd     *bufio.Reader
	state  stateFunc
	tokens chan token
	quit   chan struct{}
	err    error
}

func newLexer(quit chan struct{}, rd io.Reader) *lexer {
	return &lexer{
		rd:     bufio.NewReader(rd),
		tokens: make(chan token),
		quit:   quit,
	}
}

func lexSpace(l *lexer) stateFunc {
	spaces := 0
	for {
		r, _, err := l.rd.ReadRune()
		if err != nil {
			if spaces > 0 {
				l.emit(tokenSpace, "")
			}
			l.error(err)
			return nil
		}

		if isSpace(r) {
			spaces++
			continue
		}

		if spaces > 0 {
			l.emit(tokenSpace, "")
		}

		err = l.rd.UnreadRune()
		if err != nil {
			l.error(err)
			return nil
		}
		return lexString
	}

}

func lexEOL(l *lexer) stateFunc {
	for {
		r, _, err := l.rd.ReadRune()
		if err != nil {
			l.error(err)
			return nil
		}

		if isEOL(r) {
			if r == '\n' {
				l.emit(tokenEOL, "")
			}
		} else {
			err := l.rd.UnreadRune()
			if err != nil {
				l.error(err)
				return nil
			}
			return lexString
		}
	}
}

func lexString(l *lexer) stateFunc {
	var buf bytes.Buffer

	for {
		r, _, err := l.rd.ReadRune()
		if err != nil {
			if buf.Len() > 0 {
				l.emitValue(buf.String())
			}
			l.error(err)
			return nil
		}

		space, eol := isSpace(r), isEOL(r)
		if !space && !eol {
			_, err := buf.WriteRune(r)
			if err != nil {
				l.error(err)
				return nil
			}

		} else {
			err = l.rd.UnreadRune()
			if err != nil {
				l.error(err)
				return nil
			}

			// Anything in the buffer?
			if buf.Len() > 0 {
				l.emitValue(buf.String())
			}

			if space {
				return lexSpace
			} else {
				return lexEOL
			}
		}
	}
}

func (l *lexer) emitValue(val string) {
	_, err := strconv.ParseInt(val, 10, 64)
	if err == nil {
		l.emit(tokenNumber, val)
	} else {
		l.emit(tokenString, val)
	}
}

func (l *lexer) emit(typ tokenType, val string) {
	select {
	case l.tokens <- token{typ: typ, val: val}:
	case <-l.quit:
	}

}

func (l *lexer) error(err error) {
	if err == io.EOF {
		l.emit(tokenEOF, "")

	} else if err != nil {
		l.err = err
		l.emit(tokenError, "")
	}
}

func (l *lexer) run() {
	for l.state = lexString; l.state != nil; {
		l.state = l.state(l)
	}
}

func (l *lexer) nextToken() token {
	select {
	case <-l.quit:
		close(l.tokens)
		return token{typ: tokenQuit}
	case token := <-l.tokens:
		return token
	}
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t'
}

func isEOL(r rune) bool {
	return r == '\n' || r == '\r'
}
