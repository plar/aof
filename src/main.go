package main

import (
	"aof"
	"bufio"
	"fmt"
	"io"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stdout, `Usage: aofcompactor [FILE]
Compact AOF [FILE] or standard input to standard output.

When FILE is -, read standard input.
`)
	os.Exit(255)
}

func main() {
	var reader io.Reader
	var f *os.File
	var err error

	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		reader = bufio.NewReader(os.Stdin)
	} else {
		if len(os.Args) <= 1 {
			usage()
		}

		if os.Args[1] == "-" {
			reader = bufio.NewReader(os.Stdin)
		} else {
			f, err = os.Open(os.Args[1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Cannot open '%s' file\n", os.Args[1])
				os.Exit(1)
			}
			defer f.Close()
			reader = bufio.NewReader(f)
		}

	}

	parser := aof.NewAOFParser(reader)
	go parser.Parse()
	defer parser.Quit()

	for {
		event := parser.NextEvent()
		//fmt.Printf("Recv event=%v\n", event)
		if event.Type == aof.EventQuit || event.Type == aof.EventError || event.Type == aof.EventCompleted {
			if event.Type == aof.EventError {
				fmt.Fprintf(os.Stderr, "Cannot parse file: %s\n", parser.Error())
				os.Exit(2)
			}
			break
		}

		if (event.Type & aof.EventFinal) == aof.EventFinal {
			switch event.Type & ^aof.EventFinal {
			case aof.EventCreate, aof.EventSet, aof.EventModify:
				fmt.Fprintf(os.Stdout, "CREATE %s %d\n", event.Key, event.Value)
			case aof.EventDelete:
				fmt.Fprintf(os.Stdout, "DELETE %s\n", event.Key)
			}
		}
	}

	os.Stdout.Sync()
	os.Exit(0)
}
