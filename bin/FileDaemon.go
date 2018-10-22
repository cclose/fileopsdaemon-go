package main

import (
	configPkg "github.com/zpatrick/go-config"
	"errors"
	"fmt"
	"github.com/pborman/getopt/v2"
	"log"
	"os"
	server "FileDaemon"
	"strings"
	"time"
)

var (
	serverMode = false
	verbose = false
	configFile, execute string
	executeRetries int = 3
	executeTimeout int = 25
)
func init () {
	getopt.FlagLong(&serverMode, "server", 's', "run in server mode")
	getopt.FlagLong(&execute, "execute", 'e', "execute file operation")
	getopt.FlagLong(&configFile, "conf", 'c', "file path to config options")
	getopt.FlagLong(&verbose, "verbose", 'v', "verbose mode")
	getopt.FlagLong(&executeRetries, "retries", 'r', "how many times to retry execute requests")
	getopt.FlagLong(&executeTimeout, "timeout", 't', "how long to wait for requests before aborting")
}


func loadConfiguration(configFile string) (*server.Config, error) {
	defaultSettings := map[string]string{
		"server.timezone": "America/Chicago",
		"server.timestamp_format": "01/02/06 15:04:05.000",
		"request.socket_file": "/tmp/fd_odc_ws.sock",
		"request.message_delimiter": "|",
		"workers.number": "5",
		"workers.socket_name": "workers",
		"workers.failure_timeout": "5",
		"workers.failure_threshold": "5",
		"log.file": "stdout",
		"log.error_log": "stderr",
	}
	defaults := configPkg.NewStatic(defaultSettings)
	providers := []configPkg.Provider{defaults}//defaults first so they get overriden

	if configFile != "" {
		if _, err := os.Stat(configFile) ; os.IsNotExist(err) {
			return nil, errors.New("Configuration File does not exist: " + configFile)
		}
		iniFile := configPkg.NewINIFile(configFile)
		iniFileLoader := configPkg.NewCachedLoader(iniFile) //loads the file once. Invalidate() to reload
		providers = append(providers, iniFileLoader)
	}

	config := configPkg.NewConfig(providers)
	if err := config.Load(); err != nil {
		return nil, errors.New("Error loading config : " + err.Error())
	}

	var err error
	var errs []string
	sCon := server.Config{}

	if sCon.TimeZoneName, err = config.String("server.timezone"); err != nil {
		errs = append(errs, err.Error())
	} else { //if timezone name is defined load it
		tz, err := time.LoadLocation(sCon.TimeZoneName)
		if err != nil { //alert on invalid timezones
			errs = append(errs, err.Error())
		} else {
			sCon.TimeZone = tz
		}
	}
	if sCon.TimeStampFormat, err = config.String("server.timestamp_format"); err != nil {
		errs = append(errs, err.Error())
	}
	if sCon.RequestSocketFileName, err = config.String("request.socket_file"); err != nil {
		errs = append(errs, err.Error())
	}
	if sCon.MessageDelimiter, err = config.String("request.message_delimiter"); err != nil {
		errs = append(errs, err.Error())
	}
	if sCon.NumberOfWorkers, err = config.Int("workers.number"); err != nil {
		errs = append(errs, err.Error())
	}
	if sCon.WorkerSocketFileName, err = config.String("workers.socket_name"); err != nil {
		errs = append(errs, err.Error())
	}
	if sCon.WorkerFailureTimeout, err = config.Int("workers.failure_timeout"); err != nil {
		errs = append(errs, err.Error())
	}
	if sCon.WorkerFailureThreshold, err = config.Int("workers.failure_threshold"); err != nil {
		errs = append(errs, err.Error())
	}
	if sCon.LogFile, err = config.String("log.file"); err != nil {
		errs = append(errs, err.Error())
	}
	if sCon.ErrorLogFile, err = config.String("log.error_log"); err != nil {
		errs = append(errs, err.Error())
	}


	//finish socket paths
	//request socket is UDS
	sCon.RequestSocketFile = "ipc://" + sCon.RequestSocketFileName
	//worker socket is inproc
	sCon.WorkerSocketFile = "inproc://" + sCon.WorkerSocketFileName

	if len(errs) > 0 {
		err = errors.New("invalid configuration:\n\t" +
			strings.Join(errs, "\n\t"))
	} else {
		err = nil
	}

	return &sCon, err
}

func executeCommand(command string, config *server.Config) (err error) {
	response, err := server.SendCommand(command, config, executeRetries, executeTimeout, verbose)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Request failed: " + err.Error())
	} else {
		if response != "" {
			fmt.Println(response)
		} else {
			if verbose {
				fmt.Println("Success!")
			}
		}
	}

	return
}

func main() {
	getopt.Parse()
	if serverMode && execute != "" {
		log.Fatal("cannot execute commands in serverMode")
	}
	//Load our configuration
	var err error
	serverConfig, err := loadConfiguration(configFile)
	if err != nil {
		log.Fatal(err)
	}

	if serverMode {
		fdServer := server.NewServer(serverConfig)
		fdServer.RunServer()
		//executeCommand("chmod|write|false|" + serverConfig.RequestSocketFileName, serverConfig)
	} else if execute != "" {
		err := executeCommand(execute, serverConfig)
		if err != nil {
			os.Exit(1)
		}
	} else {
		log.Fatal("no run command (server | execute) specified!")
	}
}

