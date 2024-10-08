package main

import (
	"charlie/administration"
	"charlie/server"
	"encoding/json"
	"github.com/alecthomas/kong"
	"io"
	"log"
	"os"
)

var CLI struct {
	CreateInstance struct {
		InstanceName    string `required:"" arg:"" name:"instance-name" help:"Name of the instance"`
		CredentialsFile string `required:"" arg:"" name:"credentials-file" help:"path to a credentials file"`
	} `cmd:"" help:"Creates a new charlie instance"`

	RunServer struct {
		ConfigFile   string `required:"" name:"config"`
		LoggingLevel string `optional:"" default:"default"`
	} `cmd:"" help:"Run a profile"`
}

func main() {
	ctx := kong.Parse(&CLI)

	switch ctx.Command() {
	case "create-instance <instance-name> <credentials-file>":
		serverConfigJsonEncoded, err := administration.CreateNewInstance("127.0.0.1:8080", CLI.CreateInstance.InstanceName, CLI.CreateInstance.CredentialsFile)

		if err != nil {
			log.Printf("error creating instance: %v", err)
			return
		}

		serverConfigJsonFile, err := os.OpenFile("./server_config.json", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
		if err != nil {
			log.Printf("error creating instance configuration files: %v", err)
			return
		}

		if _, err := serverConfigJsonFile.Write(serverConfigJsonEncoded); err != nil {
			log.Printf("error saving instance configuration files: %v", err)
			return
		}

	case "run-server":
		configFile, err := os.Open(CLI.RunServer.ConfigFile)
		if err != nil {
			log.Printf("error opening config file at %s: %v", CLI.RunServer.ConfigFile, err)
			return
		}

		configFileBytes, err := io.ReadAll(configFile)
		if err != nil {
			log.Printf("error reading config file: %v", err)
			return
		}

		var serverConfig administration.ServerConfig
		if err := json.Unmarshal(configFileBytes, &serverConfig); err != nil {
			log.Printf("error unmarshalling json config file: %v", err)
			return
		}

		s, err := server.NewServer(CLI.RunServer.LoggingLevel, &serverConfig)
		if err != nil {
			log.Printf("error creating new server: %v", err)
			return
		}

		s.Run()
	}
}
