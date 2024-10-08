package administration

import (
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/crypto/bcrypt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
)

type Credentials struct {
	Password string `json:"password"`
}

type ServerConfig struct {
	ServerAddress      string `json:"server_address"`
	WorkDir            string `json:"work_dir"`
	AuthenticationHash string `json:"authentication_hash"`
}

func CreateNewInstance(serverAddress, instanceName, credentialsFilepath string) ([]byte, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	credentialsFile, err := os.Open(credentialsFilepath)
	if err != nil {
		return nil, errors.Join(errors.New("could not open credentials file"), err)
	}

	credentials, err := io.ReadAll(credentialsFile)
	if err != nil {
		return nil, errors.Join(errors.New("could not read from credentials file"), err)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(fmt.Sprintf("%s-charlie", string(credentials))), 12)
	if err != nil {
		return nil, errors.Join(errors.New("could not generate hash from credentials"), err)
	}

	if _, err := netip.ParseAddrPort(serverAddress); err != nil {
		return nil, fmt.Errorf("error parsing address: %v", err)
	}

	serverConfig := ServerConfig{
		ServerAddress:      serverAddress,
		WorkDir:            filepath.Join(homeDir, instanceName),
		AuthenticationHash: string(hash),
	}

	serverConfigJsonEncoded, err := json.MarshalIndent(serverConfig, "", "    ")
	if err != nil {
		return nil, err
	}

	return serverConfigJsonEncoded, nil
}
