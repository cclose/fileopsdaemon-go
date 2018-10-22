package FileDaemon

import (
	"time"
	"fmt"
	"os"
	"errors"
	panicUtil "github.com/cclose/go-utils/panic"
	"strings"
	zmq "github.com/pebbe/zmq4"
)

func SendCommand(command string, config *Config, executeRetries, executeTimeout int, verbose bool) (message string, reqErr error) {
	defer panicUtil.ReturnPanic(&reqErr)

	client, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		panic(err)
	}
	client.Connect(config.RequestSocketFile)

	poller := zmq.NewPoller()
	poller.Add(client, zmq.POLLIN)

	retriesLeft := executeRetries
	timeOut := time.Second * time.Duration(executeTimeout)
	if verbose {
		fmt.Printf("A: Attempting Command: %s Socket: %s Timeout %d Tries %d\n", command, config.RequestSocketFile,
			executeTimeout, executeRetries)
	}
	var reply string
	//  We send a request, then we work to get a reply
	client.SendMessage(command)

	for expectReply := true; expectReply; {
		//  Poll socket for a reply, with timeout
		sockets, err := poller.Poll(timeOut)
		if err != nil {
			break //  Interrupted
		}

		//  Here we process a server reply and exit our loop if the
		//  reply is valid. If we didn't a reply we close the client
		//  socket and resend the request. We try a number of times
		//  before finally abandoning:

		if len(sockets) > 0 {
			//  We got a reply from the server, must match sequence
			reply, err = client.Recv(0)
			if err != nil {
				break //  Interrupted
			}
			if verbose {
				fmt.Printf("I: server replied OK (%s)\n", reply)
			}
			retriesLeft = executeRetries
			expectReply = false

		} else {
			retriesLeft--
			if retriesLeft == 0 {
				errMsg :=  "E: server seems to be offline, abandoning"
				if verbose {
					fmt.Fprintln(os.Stderr, errMsg)
				}
				reqErr = errors.New(errMsg)
				break
			} else {
				if verbose {
					fmt.Fprintln(os.Stderr, "W: no response from server, retrying...")
				}
				//  Old socket is confused; close it and open a new one
				client.Close()
				client, _ = zmq.NewSocket(zmq.REQ)
				client.Connect(config.RequestSocketFile)
				// Recreate poller for new client
				poller = zmq.NewPoller()
				poller.Add(client, zmq.POLLIN)
				//  Send request again, on new socket
				client.SendMessage(command)
			}
		}

	}
	client.Close()

	if reply != "" {
		replyParts := strings.Split(reply, config.MessageDelimiter)
		success := replyParts[0]
		message = strings.Join(replyParts[1:], "\n")
		if success != "true" {
			reqErr = errors.New(message)
			message = ""
		}
	}

	return
}