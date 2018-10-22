package FileDaemon

import (
	"time"
	"log"
	"os"
	zmq "github.com/pebbe/zmq4"
	"io"
)

type Server struct {
	Active bool
	Config *Config
	ZMQContext *zmq.Context

	WorkerID int
	Workers map[int]Worker
	Notify chan int

	Log, ErrorLog *log.Logger
}

func NewServer(config *Config) (*Server) {
	server := Server{ Active: false, Config: config, WorkerID: 0 }

	server.Workers = make(map[int]Worker)
    server.Notify = make(chan int, config.NumberOfWorkers)

    var logStream io.Writer
    if server.Config.LogFile != "stdout" {
		file, err := os.OpenFile(server.Config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
		if err != nil {
			log.Fatalln("Failed to open log file " + server.Config.LogFile + ":" + err.Error())
		}
		logStream = file

	} else {
		logStream = os.Stdout
	}
	server.Log = log.New(logStream, "", 0)

	var errorLogStream io.Writer
	if server.Config.ErrorLogFile != "stderr" {
		file, err := os.OpenFile(server.Config.ErrorLogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
		if err != nil {
			log.Fatalln("Failed to open log file " + server.Config.ErrorLogFile + ":" + err.Error())
		}
		errorLogStream = file

	} else {
		errorLogStream = os.Stderr
	}
	server.ErrorLog = log.New(errorLogStream, "", 0)


	return &server
}

type Config struct {
	TimeStampFormat string
	TimeZoneName string
	TimeZone *time.Location

	NumberOfWorkers int

	RequestSocketFileName string
	RequestSocketFile     string
	WorkerSocketFileName  string
	WorkerSocketFile      string

	WorkerFailureTimeout int
	WorkerFailureThreshold int

	MessageDelimiter string
	LogFile, ErrorLogFile string
}

func (server Server) getTimeStamp() string {
	now := time.Now().In(server.Config.TimeZone)
	return now.Format(server.Config.TimeStampFormat)
}

func (server *Server) RunServer() {
	server.Active = true
	context, err := zmq.NewContext()
	if err != nil {
		log.Fatal(err)
	}
	server.ZMQContext = context

	router, err := context.NewSocket(zmq.ROUTER)
	if err != nil {
		log.Fatal(err)
	}
	defer router.Close()
	socketFile := server.Config.RequestSocketFile
	if err := router.Bind(socketFile); err != nil {
		log.Fatal(err)
	}

	//Worker Master communication socket
	workerDealer, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		log.Fatal(err)
	}
	defer workerDealer.Close()
	workerDealer.Bind(server.Config.WorkerSocketFile)



	// Create the workers
	for newWorker := 0; newWorker < server.Config.NumberOfWorkers; newWorker++ {
		 server.NewWorker()
	}

	// Connect the worker threads to the request socket via a queue
	// This is blocking, so run it in a thread
	go func() {
		err = zmq.Proxy(router, workerDealer, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	//make sure our request file is writable
	server.verifyRequestSocketFile()

	//notify about start up
	server.Log.Printf("[%s] Server Online - %s - Loaded %d Workers\n", server.getTimeStamp(), socketFile, server.Config.NumberOfWorkers)

	// Make a timer for periodic tasks we want the main thread dealing with
	tickChan := time.NewTicker(time.Second * 5).C

	//Spin while the server is active, waiting for workers to quit
	for server.Active {
		//Use a select for future expansion
		select {
		case deadID := <-server.Notify :
			delete(server.Workers, deadID) //delete the worker
			server.NewWorker()

		case <- tickChan :
			server.verifyRequestSocketFile()
		}
		//When a worker exits, we'll wake up and check that the server is active
		//Useful to note that if a worker is told to shutdown the server, it will need to also exit to prompt
		//us to check that the server is shutting down
	}

	server.Log.Printf("[%s] Server Shutdown Detected\n", server.getTimeStamp())
}

const (
	fullWrite = 0777
)
func (server *Server) verifyRequestSocketFile() {
	socketFile := server.Config.RequestSocketFileName
	fi, _ := os.Stat(socketFile)
	if fi.Mode().Perm() != fullWrite {
		server.ErrorLog.Printf("[%s] Request File not Globally Writable!\n", server.getTimeStamp())
		_, err := SendCommand("chmod|write|false|" + socketFile, server.Config, 1, 10, false)
		if err != nil {
			server.ErrorLog.Printf("[%s] Unable to correct Request SocketFile permissions: %s\n", server.getTimeStamp(), err)
		}
	}
}

func (server *Server) NewWorker() {
	nextWorker := server.WorkerID
	server.WorkerID++
	newWorker := Worker{
		ID:            nextWorker,
		Server:        server,
		Active:        true,
	}
	server.Workers[nextWorker] = newWorker
	go newWorker.Work()
}
