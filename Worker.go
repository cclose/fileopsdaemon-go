package FileDaemon

import (
	panicUtil "github.com/cclose/go-utils/panic"
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"strings"
	"time"
)

type Worker struct {
	ID            int
	Server        *Server
	requestSocket *zmq.Socket

	Active bool
}

func (worker Worker) getTimeStamp() string {
	now := time.Now().In(worker.Server.Config.TimeZone)
	return now.Format(worker.Server.Config.TimeStampFormat)
}

func (worker Worker) logMessage(message string) {
	worker.Server.Log.Printf(" -- [%s] |Worker-%d| %s", worker.getTimeStamp(), worker.ID, message)
}

func (worker Worker) logError(message string) {
	worker.Server.ErrorLog.Printf(" -!!- [%s] |Worker-%d| %s", worker.getTimeStamp(), worker.ID, message)
}

func (worker *Worker) Work() {
	defer func() {
		//catch exceptions
		if r := recover(); r != nil {
			worker.logError(fmt.Sprintf("died: %v [%s]\n", r, panicUtil.IdentifyPanic()))
		}
		worker.logMessage("retired!")

		//Notify the Worker-Master that we're quitting
		worker.Server.Notify <- worker.ID
	}()

	var err error = nil
	//Create a Reply Socket for receiving orders and sending responses
	worker.requestSocket, err = worker.Server.ZMQContext.NewSocket(zmq.REP)
	if err != nil {
		panic("failed to open Socket: " + err.Error())
	}
	defer worker.requestSocket.Close()

	//Connect the worker to the Dealer via an inproc thread
	err = worker.requestSocket.Connect(worker.Server.Config.WorkerSocketFile)
	if err != nil {
		panic("failed to bind to worker Socket: " + err.Error())
	}

	//Worker online and read to receive!
	worker.logMessage("online and listening")

	//Track consecutive errors
	errorCount := 0
	for worker.Active {
		//listen for and receive a message
		msg, err := worker.requestSocket.RecvMessage(0)
		if err != nil {
			worker.logError("error reading request: " + err.Error())
			errorCount++
			// if we have consecutive failures past our limit, abandon this worker
			if errorCount > worker.Server.Config.WorkerFailureThreshold {
				panic("Consecutive Error Threshold Exceeded")

			} else {
				//we'll take a timeout for 5 seconds after any errors
				time.Sleep(time.Duration(worker.Server.Config.WorkerFailureTimeout) * time.Second)
			}

		} else {

			//Request received
			worker.handleRequest(msg)
		}
	}
}

func (worker *Worker) handleRequest(msg []string) {
	//loop over the received message parts and join them into one
	var buffer strings.Builder
	for _, msgPart := range msg {
		buffer.WriteString(msgPart)
	}

	//requests are formatted as pipe delimited strings
	//we don't worry about message framing as ZMQ does this for us
	// General format is "command|param1|param2|param3"
	// Parameters will depend on the command issued
	cmdParts := strings.Split(buffer.String(), worker.Server.Config.MessageDelimiter)
	cmd := cmdParts[0]
	params := cmdParts[1:]

	var err error
	var reply []string
	switch cmd {
	case "checksum":
		reply, err = worker.doChecksum(params)
		break
	case "chmod":
		reply, err = worker.doChmod(params)
		break
	case "chown":
		reply, err = worker.doChown(params)
		break
	case "cp":
		reply, err = worker.doCopy(params)
		break
	case "mkdir":
		reply, err = worker.doMkdir(params)
		break
	case "mv":
		reply, err = worker.doMove(params)
		break
	case "rm":
		reply, err = worker.doRemove(params)
		break
	case "status": //status is a fast command to see if the daemon is up
		reply = []string{"true"}
		err = nil
		break
	case "shutdown": //command to turn off the server
		worker.Active = false
		worker.Server.Active = false
		break

	default:
		err = errors.New("Unsupport command '" + cmd + "'")
	}

	//clear our string buffer
	buffer.Reset()
	if err == nil {
		//Operation a success. Note this as the first part of the reply
		buffer.WriteString("true")
		//If there is additional items for the reply, concatenate them with pipes
		for _, replyChunk := range reply {
			buffer.WriteString(worker.Server.Config.MessageDelimiter)
			buffer.WriteString(replyChunk)
		} //Loop and Buffer is much faster than join

	} else {
		//If we failed, note this as the first part of the reply
		buffer.WriteString("false")
		//now concatenate the error message on with a pipe
		buffer.WriteRune('|')
		buffer.WriteString(err.Error())
	}

	errorCount := 0
	replyErr := errors.New("") //Placeholder to start the loop
	for replyErr != nil {
		//we send first so that we can disregard the initial error >.>
		_, replyErr = worker.requestSocket.Send(buffer.String(), 0)
		//now we handle any errors and loop
		if replyErr != nil { //If we fail, loop a few times
			worker.logError("error sending reply: " + replyErr.Error())
			errorCount++
			// if we have consecutive failures past our limit, abandon this worker
			if errorCount > worker.Server.Config.WorkerFailureThreshold {
				panic("Consecutive Error Threshold Exceeded")
				//this panic will be handled in work
			}
		}
	}
	//and we're done
}