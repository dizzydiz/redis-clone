package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var cache sync.Map

const addr = "0.0.0.0:6380"

func main() {
	listener, err := net.Listen("tcp", addr)
	connections := make([]*ConnectionHandler, 0)

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on tcp://%s", addr)

	commandChn := make(chan Command)
	ctx, ctxCancel := context.WithCancel(context.Background())
	wg := new(sync.WaitGroup)

	// start server worker - this should also terminate at some point
	wg.Add(1)
	go handleMessage(commandChn, ctx, wg)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("got Termination")
		_ = listener.Close()
		ctxCancel()
		// should also close all active connections
	}()

	//run in a go routine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			log.Println("New Connection", conn)

			if err != nil {
				log.Fatal(err)
			}

			handler := NewConnectionHandler(conn)
			connections = append(connections, handler)
			wg.Add(1)
			go handler.handleConnection(commandChn, ctx, wg)
		}
	}()

	wg.Wait()
	log.Println("Termination Server")
}

// Will terminate is ctx is Done, or channel is closed.
func handleMessage(actions chan Command, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case action, ok := <-actions:
			log.Println("Got Action ", action, " ok ", ok)
			if !ok {
				log.Println("no more actions, Exiting")
				return
			}

			action.handle()
		case <-ctx.Done():
			log.Println("Handle Actions got done msg")
			return
		}
	}
}

type ConnectionHandler struct {
	conn net.Conn
}

func NewConnectionHandler(conn net.Conn) *ConnectionHandler {
	return &ConnectionHandler{
		conn: conn,
	}
}

func (c *ConnectionHandler) getFromConnection(requests chan Command) {
	defer func() {
		log.Println("Closing connection", c.conn)
		c.conn.Close()
		log.Println("Closing channel", requests)
		close(requests)
	}()
	defer func() {
		if err := recover(); err != nil {
			log.Println("Recovering from error", err)
		}
	}()
	p := NewParser(c.conn)
	for {
		cmd, err := p.command()
		if err != nil {
			log.Println("Error reading command", err)
			c.conn.Write([]uint8("-ERR " + err.Error() + "\r\n"))
			break
		}

		requests <- cmd
	}
}

func (c *ConnectionHandler) handleConnection(commandChn chan Command, ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		log.Println("Exiting HandleConnection")
		wg.Done()
	}()

	incoming := make(chan Command)

	go c.getFromConnection(incoming)

	for {
		select {
		case request, ok := <-incoming:
			if !ok {
				log.Println("no more messages from connection")
				return
			}

			request.GetCommand()
			if request.cmd == QUIT {
				request.quit()
				_ = c.conn.Close()
			} else {
				commandChn <- request
			}

		case <-ctx.Done():
			log.Println("handleConnection Got Done")
			_ = c.conn.Close()
			return
		}
	}
}

func startSession(conn net.Conn, commandChn chan Command) {
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
			log.Println("Error reading command", err)
			conn.Write([]uint8("-ERR " + err.Error() + "\r\n"))
			break
		}

		fmt.Printf("from Loop: %p\n", &cmd)
		// temp - send command to action channel
		commandChn <- cmd

		// if !cmd.handle() {
		// 	break
		// }
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

type CmdType int64

const (
	UNKNOWN CmdType = iota
	ECHO
	QUIT
	GET
	SET
	INCR
	DEL
	PING
)

var cmdMap = map[string]CmdType{
	"GET":  GET,
	"SET":  SET,
	"DEL":  DEL,
	"ECHO": ECHO,
	"QUIT": QUIT,
	"INCR": INCR,
	"PING": PING,
}

type Command struct {
	args []string
	conn net.Conn
	cmd  CmdType
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

func (cmd *Command) GetCommand() CmdType {
	cmdStr := strings.ToUpper(cmd.args[0])

	cmdType, isFound := cmdMap[cmdStr]
	if !isFound {
		cmdType = UNKNOWN
	}

	cmd.cmd = cmdType

	return cmd.cmd
}

// handle Executes the command and writes the response. Returns false when the connection should be closed.
func (cmd *Command) handle() bool {
	switch cmd.cmd {
	case GET:
		return cmd.get()
	case SET:
		return cmd.set()
	case DEL:
		return cmd.del()
	case QUIT:
		return cmd.quit()
	case PING:
		return cmd.ping()
	case ECHO:
		return cmd.echo()
	case INCR:
		return cmd.incr()
	case UNKNOWN:
		log.Println("Command not supported", cmd.args[0])
		cmd.conn.Write([]uint8("-ERR unknown command '" + cmd.args[0] + "'\r\n"))
	}

	return false
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
func (cmd *Command) get() bool {
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

// incr by one if value is integer, returns the result on inc operation
// if the key is not found, set it to 1
func (cmd *Command) incr() bool {
	if len(cmd.args) != 2 {
		cmd.conn.Write([]uint8("-ERR wrong number of arguments for '" + cmd.args[0] + "' command\r\n"))
		return true
	}

	val, loaded := cache.LoadOrStore(cmd.args[1], "1")

	if loaded {
		intValue, err := strconv.Atoi(val.(string))

		if err != nil {
			cmd.conn.Write([]uint8("-ERR value is not an integer or out of range\r\n"))
			return true
		}

		intValue += 1
		cache.Store(cmd.args[1], strconv.Itoa(intValue))
		cmd.conn.Write([]uint8(fmt.Sprintf(":%d\r\n", intValue)))
		return true
	}

	cache.Store(cmd.args[1], "1")
	cmd.conn.Write([]uint8(":1\r\n"))

	return true
}

// set Stores a key and value on the cache. Optionally sets expiration on the key.
func (cmd *Command) set() bool {
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
func (cmd *Command) setExpiration(pos int) error {
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

func (cmd *Command) ping() bool {

	switch len(cmd.args) {
	case 1:
		cmd.conn.Write([]uint8("+PONG\r\n"))
	case 2:
		cmd.conn.Write([]uint8(fmt.Sprintf("$%d\r\n", len(cmd.args[1]))))
		cmd.conn.Write([]uint8(fmt.Sprintf("%s\r\n", cmd.args[1])))
	default:
		cmd.conn.Write([]uint8("-ERR wrong number of arguments for '" + cmd.args[0] + "' command\r\n"))
	}

	return true
}

func (cmd *Command) echo() bool {
	if len(cmd.args) != 2 {
		cmd.conn.Write([]uint8("-ERR wrong number of arguments for '" + cmd.args[0] + "' command\r\n"))
		return true
	}

	cmd.conn.Write([]uint8(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.args[1]), cmd.args[1])))
	return true
}
