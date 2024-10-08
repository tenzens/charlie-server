package server

import (
	"charlie/administration"
	"charlie/model"
	"charlie/utils"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"golang.org/x/crypto/bcrypt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type FileRequestResponse struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	PipeReader *io.PipeReader
}

type FileRequest struct {
	EntryId  string
	Response chan FileRequestResponse
	Context  context.Context
	Cancel   context.CancelCauseFunc
}

type FilePost struct {
	reader        *io.PipeReader
	contentLength string
}

type FileTransfer struct {
	filePost chan FilePost
}

const (
	alohaEndpoint   = "/api/v1/auth/aloha"
	refreshEndpoint = "/api/v1/auth/refresh"
	liveEndpoint    = "/api/v1/live"
	entriesEndpoint = "/api/v1/entries"
	filesEndpoint   = "/api/v1/files/"
)

type Server struct {
	config        *administration.ServerConfig
	logger        Logger
	verboseLogger Logger

	persistentStorage     PersistentStorage
	devices               []*Device
	deviceMap             map[string]*Device
	devicesMu             sync.RWMutex
	rootServer            http.ServeMux
	upgrader              websocket.Upgrader
	activeFileTransfers   map[string]*FileTransfer
	activeFileTransfersMu sync.Mutex
}

func NewServer(loggingLevel string, config *administration.ServerConfig) (*Server, error) {
	if config == nil {
		return nil, errors.New("server config cannot be nil")
	}

	var logger Logger
	var verboseLogger Logger

	switch loggingLevel {
	case "minimal":
		logger = NewNonLogger()
		verboseLogger = NewNonLogger()

	case "default":
		lg, err := NewDefaultLogger()
		if err != nil {
			return nil, err
		}

		logger = lg
		verboseLogger = NewNonLogger()

	case "verbose":
		lg, err := NewDefaultLogger()
		if err != nil {
			return nil, err
		}

		logger = lg
		verboseLogger = lg
	}

	return &Server{
		config:              config,
		logger:              logger,
		verboseLogger:       verboseLogger,
		persistentStorage:   &SqlitePersistentStorage{},
		deviceMap:           make(map[string]*Device),
		activeFileTransfers: make(map[string]*FileTransfer),
		devices:             nil,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}, nil
}

func (s *Server) initialize() error {
	if _, err := os.Stat(s.config.WorkDir); os.IsNotExist(err) {
		err := os.MkdirAll(s.config.WorkDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	if err := s.persistentStorage.Initialize(s.config.WorkDir); err != nil {
		return err
	}

	devices, err := s.persistentStorage.LoadDevices()

	if err != nil {
		log.Printf("error loading devices: %v", err)
		return err
	}

	s.devices = devices
	for _, device := range devices {
		s.deviceMap[device.UUID] = device
		device.downstreamChangesBuffer.Lock()
		device.downstreamChangesBuffer.UnsafeFreeze()
		device.downstreamChangesBuffer.UnsafeNotify()
		device.downstreamChangesBuffer.Unlock()
	}

	return nil
}

func (s *Server) Run() {
	if err := s.initialize(); err != nil {
		log.Printf("Error initializing server: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.runRootServer()
	go s.runHousekeeping()

	<-ctx.Done()
}

func (s *Server) runRootServer() {
	s.rootServer.HandleFunc(alohaEndpoint, s.handleAlohaRequest)
	s.rootServer.HandleFunc(refreshEndpoint, s.handleRefreshRequest)
	s.rootServer.HandleFunc(liveEndpoint, s.handleLiveWebsocket)
	s.rootServer.HandleFunc(entriesEndpoint, s.handleEntriesRequest)
	s.rootServer.HandleFunc(filesEndpoint, s.handleFilesRequest)

	ls, err := net.Listen("tcp", s.config.ServerAddress)

	if err != nil {
		log.Printf("Error starting main server: %v", err)
		return
	}

	log.Printf("Listening on: %s", s.config.ServerAddress)
	log.Printf("Got the following err serving http: %v", http.Serve(ls, &s.rootServer))
}

func (s *Server) runHousekeeping() {
	//TODO: should delete devices that are expired based on their refresh token
	ticker := time.NewTimer(time.Hour * 24)

	select {
	case <-ticker.C:

	}
}

func (s *Server) handleAlohaRequest(w http.ResponseWriter, request *http.Request) {
	s.logger.Printf("Got an http request on %s", alohaEndpoint)

	if request.Method != http.MethodPost {
		errorMessage := fmt.Sprintf("method %s not allowed", request.Method)

		s.logger.Printf("aloha-request: %s", errorMessage)
		http.Error(w, errorMessage, http.StatusMethodNotAllowed)
		return
	}

	var password = request.FormValue("password")
	s.verboseLogger.Printf("aloha-request: Comparing aloha password %s with stored authentication hash %s", password, s.config.AuthenticationHash)

	err := bcrypt.CompareHashAndPassword([]byte(s.config.AuthenticationHash), []byte(fmt.Sprintf("%s-charlie", password)))
	if err != nil {
		s.logger.Printf("aloha-request: Unauthorized, bcrypt comparison of password and hash failed: %v", err)
		http.Error(w, "Unauthorized request", http.StatusForbidden)
		return
	}

	aloha, err := NewAloha(uuid.New().String())
	if err != nil {
		errorMessage := fmt.Sprintf("Internal server error, unable to create aloha response: %v", err)

		s.logger.Printf("aloha-request: %s", errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	newDevice, err := NewDevice(-1, aloha.DeviceId, "REPLICA", aloha.AuthToken, 5)

	if err != nil {
		errorMessage := fmt.Sprintf("Internal Server Error, unable to create new device: %v", err)

		s.logger.Printf("aloha-request: %s", errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	s.devicesMu.Lock()
	defer s.devicesMu.Unlock()

	s.devices = append(s.devices, newDevice)
	s.deviceMap[newDevice.UUID] = newDevice

	// new entries will be immediately stored to disk for a replica
	// freezing the device will cause handleReplicaDownstreamConn to load unsent changes stored on disk
	// into the buffer
	// freezing the device for mirror devices will prevent changes to be stored for that device before they connect
	// as they will only need to be informed of changes while they are connected
	newDevice.downstreamChangesBuffer.UnsafeFreeze()

	if err := s.persistentStorage.AddDevice(newDevice); err != nil {
		errorMessage := fmt.Sprintf("Internal server error, unable to add device to persistent storage: %v", err)
		s.logger.Printf("aloha-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	if err := s.persistentStorage.CopyReplica(newDevice); err != nil {
		errorMessage := fmt.Sprintf("Internal server error, unable to copy replica to new device: %v", err)
		s.logger.Printf("aloha-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	newDevice.downstreamChangesBuffer.UnsafeNotify()

	serializedAloha, err := json.MarshalIndent(aloha, "", "    ")
	if err != nil {
		errorMessage := fmt.Sprintf("Internal server error, unable to serialize auth token: %v", err)
		s.logger.Printf("aloha-request: %s", errorMessage)
		http.Error(w, errorMessage, http.StatusBadRequest)
		return
	}

	w.Write(serializedAloha)

	s.logger.Printf("aloha-request: Added new device: %s %s %s %s", newDevice.UUID, newDevice.DeviceType, newDevice.AuthToken.AccessToken, newDevice.AuthToken.RefreshToken)
}

func (s *Server) isAuthorized(request *http.Request, tokenType TokenType) (*Device, error) {
	bearerTokenBase64 := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	bearerToken, err := utils.DecodeBase64([]byte(bearerTokenBase64))

	if err != nil {
		errorMessage := fmt.Sprintf("Error decoding bearer token from base64: %v", err)
		s.logger.Printf("auth-check: %s", errorMessage)

		return nil, fmt.Errorf("error unauthorized")
	}

	s.verboseLogger.Printf("auth-check: Got auth header with %s", bearerToken)
	idtokensplit := strings.Split(string(bearerToken), "$")

	if len(idtokensplit) != 2 {
		s.logger.Printf("auth-check: Bearer token does not consist of device id and auth token (len(idtokensplit) != 2)")
		return nil, fmt.Errorf("error unauthorized")
	}

	s.devicesMu.RLock()
	var device *Device
	if device = s.deviceMap[idtokensplit[0]]; device == nil {
		s.devicesMu.RUnlock()

		s.logger.Printf("auth-check: Error device doesn't exist")
		return nil, fmt.Errorf("error unauthorized")
	}
	s.devicesMu.RUnlock()

	device.authMu.Lock()
	defer device.authMu.Unlock()

	var token string
	var expirationDate time.Time
	if tokenType == AccessTokenType {
		s.verboseLogger.Printf("auth-check: Token type is access token")
		token = device.AuthToken.AccessToken
		expirationDate = time.UnixMilli(device.AuthToken.AccessTokenExpiration)
	} else if tokenType == RefreshTokenType {
		s.verboseLogger.Printf("auth-check: Token type is refresh token")
		token = device.AuthToken.RefreshToken
		expirationDate = time.UnixMilli(device.AuthToken.RefreshTokenExpiration)
	} else {
		s.logger.Printf("auth-check: Error unknown auth token type")
		return nil, fmt.Errorf("error unknown auth token type")
	}

	s.verboseLogger.Printf("auth-check: Got request with token: %s and stored token: %s", idtokensplit[1], token)
	if token != idtokensplit[1] {
		s.logger.Printf("auth-check: Token not correct for device")
		return nil, fmt.Errorf("error unauthorized")
	}

	if time.Now().After(expirationDate) {
		s.logger.Printf("auth-check: token expired")
		return nil, fmt.Errorf("error unauthorized")
	}

	return device, nil
}

func (s *Server) handleRefreshRequest(w http.ResponseWriter, request *http.Request) {
	s.logger.Printf("Got an http request on %s", refreshEndpoint)

	device, err := s.isAuthorized(request, RefreshTokenType)
	if err != nil {
		if errors.Is(err, errors.New("error unauthorized")) {
			http.Error(w, "Unauthorized request", http.StatusForbidden)
			return
		} else {
			http.Error(w, fmt.Sprintf("Bad request: %v", err), http.StatusBadRequest)
			return
		}
	}

	if request.Method != http.MethodGet {
		errorMessage := fmt.Sprintf("Method %s not allowed", request.Method)

		s.logger.Printf("refresh-request: %s", errorMessage)
		http.Error(w, errorMessage, http.StatusMethodNotAllowed)
		return
	}

	authToken, err := NewAuthToken()
	if err != nil {
		errorMessage := fmt.Sprintf("Internal server error, unable to create new authentication token: %v", err)
		s.logger.Printf("refresh-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	serializedAuthToken, err := json.MarshalIndent(authToken, "", "    ")
	if err != nil {
		errorMessage := fmt.Sprintf("Internal server error, unable to marshal auth token: %v", err)
		s.logger.Printf("refresh-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	device.authMu.Lock()
	defer device.authMu.Unlock()

	if err := s.persistentStorage.UpdateDeviceAuthToken(device, authToken); err != nil {
		errorMessage := fmt.Sprintf("Internal server error, unable to update auth token for device %s: %v", device.UUID, err)
		s.logger.Printf("refresh-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}
	device.AuthToken = authToken

	s.logger.Printf("refresh-request: Writing serialized refreshed auth token")
	w.Write(serializedAuthToken)
}

func (s *Server) handleEntriesRequest(w http.ResponseWriter, request *http.Request) {
	s.logger.Printf("Got an http request on %s", entriesEndpoint)

	device, err := s.isAuthorized(request, AccessTokenType)
	if err != nil {
		if errors.Is(err, errors.New("error unauthorized")) {
			http.Error(w, "Unauthorized request", http.StatusForbidden)
			return
		} else {
			http.Error(w, fmt.Sprintf("Bad request: %v", err), http.StatusBadRequest)
			return
		}
	}

	if request.Method == http.MethodPost {
		s.postEntriesRequest(device, w, request)
	} else {
		errorMessage := fmt.Sprintf("method %s not allowed", request.Method)
		s.logger.Printf("entries-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusMethodNotAllowed)
	}
}

func (s *Server) postEntriesRequest(device *Device, w http.ResponseWriter, request *http.Request) {
	serializedCommit, err := io.ReadAll(request.Body)

	if err != nil {
		errorMessage := fmt.Sprintf("Bad request, unable to read request body: %v", err)
		s.logger.Printf("post-entries-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusBadRequest)
		return
	}

	s.verboseLogger.Printf("post-entries-request: Got the following commit: %s", serializedCommit)

	var commit model.Commit
	if err := json.Unmarshal(serializedCommit, &commit); err != nil {
		errorMessage := fmt.Sprintf("Bad request, unable to unmarshal commit: %v")
		s.logger.Printf("post-entries-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusBadRequest)
		return
	}

	for _, change := range commit.Changes {
		var entryUUID string
		switch change.Operation {
		case "ADD":
			entryUUID = change.Entry.UUID

		case "REMOVE":
			entryUUID = change.EntryUUID
		}

		if _, err := uuid.Parse(entryUUID); err != nil {
			errorMessage := fmt.Sprintf("Bad request, entry id is not a uuid: %v", err)
			s.logger.Printf("post-entries-request: %s", errorMessage)

			http.Error(w, errorMessage, http.StatusBadRequest)
			return
		}
	}

	s.devicesMu.RLock()
	defer s.devicesMu.RUnlock()

	peers := calculatePeers(device.UUID, s.devices)
	peersString := make([]string, len(peers))
	for i, peer := range peers {
		peersString[i] = peer.UUID
	}

	if err := s.persistentStorage.PersistChangesForPeers(device, peers, commit.Changes); err != nil {
		errorMessage := fmt.Sprintf("Internal server error, unable to persist changes for peers: %v", err)
		s.logger.Printf("post-entries-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	if err := s.persistentStorage.Replicate(commit.Changes); err != nil {
		errorMessage := fmt.Sprintf("Internal server error, unable to replicate changes: %v", err)
		s.logger.Printf("post-entries-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	s.distributeCommitDownstream(peers, commit)
	s.verboseLogger.Printf("post-entries-request: Distributed commit downstream")

	w.WriteHeader(http.StatusOK)
	s.verboseLogger.Printf("post-entries-request: Successfully handled request")
}

func (s *Server) handleFilesRequest(w http.ResponseWriter, request *http.Request) {
	s.logger.Printf("Got an http request on %s", filesEndpoint)

	device, err := s.isAuthorized(request, AccessTokenType)
	if err != nil && errors.Is(err, errors.New("error unauthorized")) {
		http.Error(w, "Unauthorized request", http.StatusForbidden)
		return
	} else if err != nil {
		http.Error(w, fmt.Sprintf("Bad request: %v", err), http.StatusBadRequest)
		return
	}

	path := request.URL.Path

	if strings.HasPrefix(path, "/") {
		path = path[1:]
	}
	parts := strings.Split(path, "/")

	// /api/v1/files/<fileid>
	if len(parts) < 4 {
		s.logger.Printf("file-request: path %s incorrect", path)
		http.Error(w, "Invalid URL", http.StatusNotFound)
		return
	}
	entryId := parts[3]

	if request.Method == http.MethodGet {
		s.handleFilesGet(device, entryId, w, request)
	} else if request.Method == http.MethodPost {
		s.handleFilesPost(entryId, w, request)
	} else {
		errorMessage := fmt.Sprintf("Method %s not allowed", request.Method)
		s.logger.Printf("file-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleFilesGet(sourceDevice *Device, entryId string, w http.ResponseWriter, request *http.Request) {
	s.logger.Printf("file-request: Got file request GET for %s", entryId)

	entry, err := s.persistentStorage.LoadEntry(entryId)

	if err != nil {
		errorMessage := fmt.Sprintf("Bad request, entry does not exist: %v", err)
		s.logger.Printf("get-file-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusNotFound)
		return
	}

	s.verboseLogger.Printf("get-file-request: Got entry for file request get %s", entry.UUID)

	targetDevice := s.deviceMap[entry.DeviceUUID]
	if targetDevice == nil {
		errorMessage := fmt.Sprintf("Bad request, target device does not exist")
		s.logger.Printf("get-file-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusBadRequest)
		return
	}

	s.activeFileTransfersMu.Lock()
	if s.activeFileTransfers[entryId] != nil {
		s.activeFileTransfersMu.Unlock()
		errorMessage := fmt.Sprintf("Too many requests, there is already a get on this file")
		s.logger.Printf("get-file-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusTooManyRequests)
		return
	}

	fileTransfer := &FileTransfer{
		filePost: make(chan FilePost),
	}

	s.activeFileTransfers[entryId] = fileTransfer
	s.activeFileTransfersMu.Unlock()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	fileRequest := FileRequest{
		EntryId:  entry.UUID,
		Response: make(chan FileRequestResponse),
		Context:  ctx,
		Cancel:   cancel,
	}

	s.verboseLogger.Printf("get-file-request: Notifying target device of file request")

	select {
	case targetDevice.fileRequestChan <- fileRequest:
	case <-time.After(5 * time.Second):
		errorMessage := fmt.Sprintf("Timeout waiting for target device")
		s.logger.Printf("get-file-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusRequestTimeout)
		return
	}

	s.verboseLogger.Printf("get-file-request: Notifyed target device of file request")

	select {
	case <-ctx.Done():
		s.logger.Printf("get-file-request: Context is done for file request %v", ctx.Err())
		http.Error(w, "Bad request", http.StatusBadRequest)
	case <-time.After(10 * time.Second):
		errorMessage := fmt.Sprintf("Timeout, target device not responding to file request")
		s.logger.Printf("get-file-request: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusRequestTimeout)
	case fileRequestResponse := <-fileRequest.Response:
		s.logger.Printf("get-file-request: Got file request response from target device: %d %s", fileRequestResponse.Code, fileRequestResponse.Message)

		if fileRequestResponse.Code != http.StatusOK {
			http.Error(w, fmt.Sprintf("Bad request"), http.StatusBadRequest)
			return
		}

		select {
		case <-time.After(5 * time.Second):
			errorMessage := fmt.Sprintf("Timeout waiting for corresponding post request")
			s.logger.Printf("get-file-request: %s", errorMessage)

			http.Error(w, errorMessage, http.StatusRequestTimeout)
		case filePost := <-fileTransfer.filePost:
			s.logger.Printf("get-file-request: Got corresponding file post request")

			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", filePost.contentLength)

			_, err := io.Copy(w, filePost.reader)

			if err != nil {
				s.logger.Printf("get-file-request: Got error reading from file post request: %v", err)
			} else {
				s.logger.Printf("get-file-request: Successfully wrote file")
			}
		}
	}

	s.activeFileTransfersMu.Lock()
	delete(s.activeFileTransfers, entryId)
	s.activeFileTransfersMu.Unlock()
}

func (s *Server) handleFilesPost(entryId string, w http.ResponseWriter, request *http.Request) {
	s.logger.Printf("file-request: Got file request POST for %s", entryId)

	s.activeFileTransfersMu.Lock()
	activeFileTransfer := s.activeFileTransfers[entryId]

	if activeFileTransfer == nil {
		errorMessage := fmt.Sprintf("Could not find corresponding file get request")
		s.logger.Printf("file-request-post: %s", errorMessage)

		http.Error(w, errorMessage, http.StatusBadRequest)
		s.activeFileTransfersMu.Unlock()
		return
	}

	s.activeFileTransfersMu.Unlock()

	s.verboseLogger.Printf("file-request-post: Post request is for active file request")

	reader, writer := io.Pipe()

	contentLength := request.Header.Get("Content-Length")

	activeFileTransfer.filePost <- FilePost{
		reader:        reader,
		contentLength: contentLength,
	}

	s.verboseLogger.Printf("file-request-post: Writing file")

	_, err := io.Copy(writer, request.Body)

	if err != nil {
		s.logger.Printf("file-request-post: Error writing file: %v", err)
		_ = writer.CloseWithError(err)
	} else {
		s.logger.Printf("file-request-post: Successfully posted file")
		_ = writer.Close()
	}
}

func (s *Server) handleLiveWebsocket(w http.ResponseWriter, request *http.Request) {
	s.logger.Printf("Got an http request on %s", liveEndpoint)
	device, err := s.isAuthorized(request, AccessTokenType)

	if err != nil && errors.Is(err, errors.New("error unauthorized")) {
		http.Error(w, "Unauthorized request", http.StatusForbidden)
		return
	} else if err != nil {
		http.Error(w, fmt.Sprintf("Bad request: %v", err), http.StatusBadRequest)
		return
	}

	conn, err := s.upgrader.Upgrade(w, request, nil)
	if err != nil {
		log.Println(err)
		return
	}

	defer conn.Close()

	msgType, msg, err := conn.ReadMessage()
	if err != nil || msgType == websocket.BinaryMessage {
		s.logger.Printf("live-request: Error reading command: %v", err)
		return
	}

	pushableConnRe := regexp.MustCompile(`^PUSHABLE$`)
	switch {
	case pushableConnRe.MatchString(string(msg)):
		if err := conn.WriteMessage(websocket.TextMessage, []byte("200 OK")); err != nil {
			s.logger.Printf("live-request: Error writing message to live conn: %v", err)
			return
		}

		s.handleReplicaDownstreamConn(conn, device)
	default:
		_ = conn.WriteMessage(websocket.TextMessage, []byte("500 Syntax error, command unrecognized"))
	}
}

func calculatePeers(deviceId string, devices []*Device) []*Device {
	exclude := -1
	for i, device := range devices {
		if device.UUID == deviceId {
			exclude = i
		}
	}

	if exclude < 0 {
		return []*Device{}
	}

	peersBefore := append([]*Device{}, devices[:exclude]...)
	peersAfter := append([]*Device{}, devices[exclude+1:]...)
	peers := append(peersBefore, peersAfter...)

	return peers
}

func (s *Server) distributeCommitDownstream(destDevices []*Device, commit model.Commit) {
	for _, device := range destDevices {
		_ = device.downstreamChangesBuffer.Append(commit.Changes)
	}
}

func (s *Server) handleReplicaDownstreamConn(conn *websocket.Conn, device *Device) {
	s.logger.Printf("downstream-conn: Got downstream connection for %s", device.UUID)

	device.downstreamConnection <- struct{}{}
	ticker := time.NewTicker(100 * time.Second)

outer:
	for {
		select {
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(3*time.Second)); err != nil {
				break outer
			}
		case fileRequest := <-device.fileRequestChan:
			if err := s.fileRequest(conn, fileRequest); err != nil {
				break outer
			}
		case <-device.downstreamChangesBuffer.Notification:
			s.verboseLogger.Printf("downstream-conn: Took notification off for device: %s", device.UUID)
			device.downstreamChangesBuffer.Lock()
			s.verboseLogger.Printf("downstream-conn: Got lock in handleReplicaDownstreamConn for device: %s", device.UUID)

			device.downstreamChangesBuffer.UnsafeSetNotified(false)

			isFrozen := device.downstreamChangesBuffer.UnsafeIsFrozen()
			changes := device.downstreamChangesBuffer.UnsafePeek()

			if len(changes) > 0 {
				for _, change := range changes {
					s.verboseLogger.Printf("downstream-conn: pushing change %d with operation %s", change.Id, change.Operation)
				}

				if err := s.pushChangesDownstream(conn, changes); err != nil {
					s.logger.Printf("downstream-conn: Error pushing changes downstream to device %s %v", device.UUID, err)
					device.downstreamChangesBuffer.UnsafeNotify()
					device.downstreamChangesBuffer.Unlock()
					break outer
				}

				s.verboseLogger.Printf("downstream-conn: Successfully pushed changes downstream: %s", device.UUID)
				if err := s.persistentStorage.RemoveDeliveredChanges(device, changes); err != nil {
					s.logger.Printf("downstream-conn: Error marking changes as delivered: %v", err)
				}

				device.downstreamChangesBuffer.UnsafeConsume()
			}

			if isFrozen {
				s.verboseLogger.Printf("downstream-conn: Device %s is frozen", device.UUID)

				undeliveredChanges, err := s.persistentStorage.LoadUndeliveredChanges(device, device.downstreamChangesBuffer.Capacity)
				if err != nil {
					s.logger.Printf("downstream-conn: Error loading undelivered changes: %v", err)
					device.downstreamChangesBuffer.UnsafeNotify()
					device.downstreamChangesBuffer.Unlock()
					continue
				}

				device.downstreamChangesBuffer.UnsafeSetSlice(undeliveredChanges)

				if len(undeliveredChanges) < device.downstreamChangesBuffer.Capacity {
					device.downstreamChangesBuffer.UnsafeDefreeze()
					s.verboseLogger.Printf("downstream-conn: Device %s is no longer frozen", device.UUID)
				}

				device.downstreamChangesBuffer.UnsafeNotify()
			}

			device.downstreamChangesBuffer.Unlock()
			s.verboseLogger.Printf("downstream-conn: Released lock in handleReplicaDownstreamConn for device: %s", device.UUID)
		}
	}

	s.logger.Printf("downstream-conn: Device %s: downstream connection closed", device.UUID)
	<-device.downstreamConnection
}

func (s *Server) pushChangesDownstream(conn *websocket.Conn, changes []model.Change) error {
	commit := model.Commit{
		Changes: changes,
	}

	serializedCommit, err := json.MarshalIndent(commit, "", "    ")
	if err != nil {
		return err
	}

	command := fmt.Sprintf("PUSH\n%s", serializedCommit)
	if err := conn.WriteMessage(websocket.TextMessage, []byte(command)); err != nil {
		return err
	}

	msgType, msg, err := conn.ReadMessage()
	if err != nil {
		return err
	}

	if msgType != websocket.TextMessage {
		return fmt.Errorf("unexpected message type")
	}

	if string(msg) != "200 OK" {
		return fmt.Errorf("command unsuccessful")
	}

	return nil
}

func (s *Server) fileRequest(conn *websocket.Conn, fileRequest FileRequest) error {
	if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("FILEREQUEST %s\n", fileRequest.EntryId))); err != nil {
		return err
	}

	msgType, content, err := conn.ReadMessage()
	if err != nil {
		return err
	}

	if msgType != websocket.TextMessage {
		fileRequest.Cancel(fmt.Errorf("error unexpected message type"))
		return nil
	}

	s.verboseLogger.Printf("live-file-request: Got the following filerequest response: %s", content)

	var code int
	var msg string
	_, err = fmt.Sscanf(string(content), "%d %s", &code, &msg)
	if err != nil {
		return err
	}

	var fileRequestReponse FileRequestResponse
	fileRequestReponse.Code = code
	fileRequestReponse.Message = msg

	fileRequest.Response <- fileRequestReponse
	return nil
}
