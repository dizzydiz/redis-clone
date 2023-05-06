package main

import (
	"bufio"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

var cache sync.Map

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6380")

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Listening on tcp://0.0.0.0:6380")

	for {
		conn, err := listener.Accept()
		log.Println("New Connection", conn)

		if err != nil {
			log.Fatal(err)
		}

		go startSession(conn)
	}
}

func startSession(conn net.Conn) {
	defer func() {
		log.Println("Closing connection", conn)
		conn.Close()
	}()

	defer func() {
		if err := recover(); err != nil {
			log.Println("Recovering from error", err)
		}
	}()

	p := NewParser(conn)

	for {
		cmd, err := p.command()

		if err != nil {
			log.Println("Error", err)
			conn.Write([]uint8("-ERR " + err.Error() + "\r\n"))
			break
		}

		if !cmd.handle() {
			break
		}
	}
}

type Parser struct {
	conn net.Conn
	r    *bufio.Reader
	line []byte
	pos  int
}

type Command struct {
	args []string
	conn net.Conn
}

func NewParser(conn net.Conn) *Parser {
	return &Parser{
		conn: conn,
		r:    bufio.NewReader(conn),
		line: make([]byte, 0),
		pos:  0,
	}
}

func (p *Parser) current() byte {
	if p.atEnd() {
		return '\r'
	}

	return p.line[p.pos]
}

func (p *Parser) advance() {
	p.pos += 1
}

func (p *Parser) atEnd() bool {
	return p.pos >= len(p.line)
}

func (p *Parser) readLine() ([]byte, error) {
	line, err := p.r.ReadBytes('\r')
	if err != nil {
		return nil, err
	}

	if _, err := p.r.ReadByte(); err != nil {
		return nil, err
	}

	return line[:len(line)-1], nil
}

func (p *Parser) consumeString() (s []byte, err error) {
	for p.current() != '"' && !p.atEnd() {
		curr := p.current()
		p.advance()
		next := p.current()

		if curr == '\\' && next == '"' {
			s = append(s, '"')
			p.advance()
		} else {
			s = append(s, curr)
		}
	}

	if p.current() != '"' {
		return nil, errors.New("Unbalanced quotes in request")
	}

	p.advance()
	return
}

func (p *Parser) command() (Command, error) {
	b, err := p.r.ReadByte()

	if err != nil {
		return Command{}, err
	}

	if b == '*' {
		log.Println("RESP Array")
		return p.respArray()
	} else {
		line, err := p.readLine()
		if err != nil {
			return Command{}, err
		}

		p.pos = 0
		p.line = append([]byte{}, b)
		p.line = append(p.line, line...)
		return p.inline()
	}
}

func (p *Parser) inline() (Command, error) {
	for p.current() == ' ' {
		p.advance()
	}

	cmd := Command{conn: p.conn}

	for !p.atEnd() {
		arg, err := p.consumeArg()
		if err != nil {
			return cmd, err
		}

		if arg != "" {
			cmd.args = append(cmd.args, arg)
		}
	}

	return cmd, nil
}

func (p *Parser) consumeArg() (s string, err error) {
	for p.current() != ' ' {
		p.advance()
	}

	if p.current() == '"' {
		p.advance()
		buf, err := p.consumeString()
		return string(buf), err
	}

	for !p.atEnd() && p.current() != ' ' && p.current != '\r' {
		s += string(p.current())
		p.advance()
	}

	return
}

func (p *Parser) respArray() (Command, error) {
	cmd := Command{}
	elementsStr, err := p.readLine()
	if err != nil {
		return cmd, err
	}

	elements, _ := strconv.Atoi(string(elementsStr))
	log.Println("Elements", elements)
	for i := 0; i < elements; i++ {
		tp, err := p.r.ReadByte()
		if err != nil {
			return cmd, err
		}
		switch tp {
		case ':':
			arg, err := p.readLine()
			if err != nil {
				return cmd, err
			}
			cmd.args = append(cmd.args, string(arg))
		case '$':
			arg, err := p.readLine()
			if err != nil {
				return cmd, err
			}
			length, _ := strconv.Atoi(string(arg))
			text := make([]byte, 0)
			for i := 0; len(text) <= length; i++ {
				line, err := p.readLine()
				if err != nil {
					return cmd, err
				}
				text = append(text, line...)
			}
			cmd.args = append(cmd.args, string(text[:length]))
		case '*':
			next, err := p.respArray()
			if err != nil {
				return cmd, err
			}
			cmd.args = append(cmd.args, next.args...)
		}
	}
	return cmd, nil
}

// handle Executes the command and writes the response. Returns false when the connection should be closed.
func (cmd Command) handle() bool {
	switch strings.ToUpper(cmd.args[0]) {
	case "GET":
		return cmd.get()
	case "SET":
		return cmd.set()
	case "DEL":
		return cmd.det()
	case "QUIT":
		return cmd.quit()
	default:
		log.Println("Command not supported", cmd.args[0])
		cmd.conn.Write([]uint8("-ERR unknown command '" + cmd.args[0] + "'\r\n"))
	}

	return true
}
