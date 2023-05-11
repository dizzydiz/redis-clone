package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
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

// startSession handles the client's session. Parses and executes commands and writes
// responses back to the client.
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

// Parser contains the logic to read from a raw tcp connection and parse commands.
type Parser struct {
	conn net.Conn
	r    *bufio.Reader
	// Used for inline parsing
	line []byte
	pos  int
}

type Command struct {
	args []string
	conn net.Conn
}

// NewParser returns a new Parser that reads from the given connection.
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
	p.pos++
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

// consumeString reads a string argument from the current line.
func (p *Parser) consumeString() (s []byte, err error) {
	for p.current() != '"' && !p.atEnd() {
		cur := p.current()
		p.advance()
		next := p.current()
		if cur == '\\' && next == '"' {
			s = append(s, '"')
			p.advance()
		} else {
			s = append(s, cur)
		}
	}
	if p.current() != '"' {
		return nil, errors.New("unbalanced quotes in request")
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
		log.Println("resp array")
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

// inline parses an inline message and returns a Command. Returns an error when there's
// a problem reading from the connection or parsing the command.
func (p *Parser) inline() (Command, error) {
	// skip initial whitespace if any
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

// consumeArg reads an argument from the current line.
func (p *Parser) consumeArg() (s string, err error) {
	for p.current() == ' ' {
		p.advance()
	}
	if p.current() == '"' {
		p.advance()
		buf, err := p.consumeString()
		return string(buf), err
	}
	for !p.atEnd() && p.current() != ' ' && p.current() != '\r' {
		s += string(p.current())
		p.advance()
	}
	return
}

// respArray parses a RESP array and returns a Command. Returns an error when there's
// a problem reading from the connection.
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
			line, err := p.readLine()
			if err != nil {
				return cmd, err
			}

			cmd.args = append(cmd.args, string(line[:length]))
		case '*':
			next, err := p.respArray()
			if err != nil {
				return cmd, err
			}
			cmd.args = append(cmd.args, next.args...)
		}
	}
	cmd.conn = p.conn
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
		return cmd.del()
	case "QUIT":
		return cmd.quit()
	default:
		log.Println("Command not supported", cmd.args[0])
		cmd.conn.Write([]uint8("-ERR unknown command '" + cmd.args[0] + "'\r\n"))
	}
	return true
}

// quit Used in interactive/inline mode, instructs the server to terminate the connection.
func (cmd *Command) quit() bool {
	if len(cmd.args) != 1 {
		cmd.conn.Write([]uint8("-ERR wrong number of arguments for '" + cmd.args[0] + "' command\r\n"))
		return true
	}
	log.Println("Handle QUIT")
	cmd.conn.Write([]uint8("+OK\r\n"))
	return false
}

// del Deletes a key from the cache.
func (cmd *Command) del() bool {
	count := 0
	for _, k := range cmd.args[1:] {
		if _, ok := cache.LoadAndDelete(k); ok {
			count++
		}
	}
	cmd.conn.Write([]uint8(fmt.Sprintf(":%d\r\n", count)))
	return true
}

// get Fetches a key from the cache if exists.
func (cmd Command) get() bool {
	if len(cmd.args) != 2 {
		cmd.conn.Write([]uint8("-ERR wrong number of arguments for '" + cmd.args[0] + "' command\r\n"))
		return true
	}
	log.Println("Handle GET")
	val, _ := cache.Load(cmd.args[1])
	if val != nil {
		res, _ := val.(string)
		if strings.HasPrefix(res, "\"") {
			res, _ = strconv.Unquote(res)
		}
		log.Println("Response length", len(res))
		cmd.conn.Write([]uint8(fmt.Sprintf("$%d\r\n", len(res))))
		cmd.conn.Write(append([]uint8(res), []uint8("\r\n")...))
	} else {
		cmd.conn.Write([]uint8("$-1\r\n"))
	}
	return true
}

// set Stores a key and value on the cache. Optionally sets expiration on the key.
func (cmd Command) set() bool {
	if len(cmd.args) < 3 || len(cmd.args) > 6 {
		cmd.conn.Write([]uint8("-ERR wrong number of arguments for '" + cmd.args[0] + "' command\r\n"))
		return true
	}
	log.Println("Handle SET")
	log.Println("Value length", len(cmd.args[2]))
	if len(cmd.args) > 3 {
		pos := 3
		option := strings.ToUpper(cmd.args[pos])
		switch option {
		case "NX":
			log.Println("Handle NX")
			if _, ok := cache.Load(cmd.args[1]); ok {
				cmd.conn.Write([]uint8("$-1\r\n"))
				return true
			}
			pos++
		case "XX":
			log.Println("Handle XX")
			if _, ok := cache.Load(cmd.args[1]); !ok {
				cmd.conn.Write([]uint8("$-1\r\n"))
				return true
			}
			pos++
		}
		if len(cmd.args) > pos {
			if err := cmd.setExpiration(pos); err != nil {
				cmd.conn.Write([]uint8("-ERR " + err.Error() + "\r\n"))
				return true
			}
		}
	}
	cache.Store(cmd.args[1], cmd.args[2])
	cmd.conn.Write([]uint8("+OK\r\n"))
	return true
}

// setExpiration Handles expiration when passed as part of the 'set' command.
func (cmd Command) setExpiration(pos int) error {
	option := strings.ToUpper(cmd.args[pos])
	value, _ := strconv.Atoi(cmd.args[pos+1])
	var duration time.Duration
	switch option {
	case "EX":
		duration = time.Second * time.Duration(value)
	case "PX":
		duration = time.Millisecond * time.Duration(value)
	default:
		return fmt.Errorf("expiration option is not valid")
	}
	go func() {
		log.Printf("Handling '%s', sleeping for %v\n", option, duration)
		time.Sleep(duration)
		cache.Delete(cmd.args[1])
	}()
	return nil
}
