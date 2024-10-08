package server

import (
	"charlie/model"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"path/filepath"
	"sync"
)

type PersistentStorage interface {
	Initialize(workdir string) error
	RemoveDeliveredChanges(device *Device, changes []model.Change) error
	LoadUndeliveredChanges(device *Device, n int) ([]model.Change, error)
	PersistChangesForPeers(device *Device, peers []*Device, changes []model.Change) error

	AddDevice(*Device) error
	Replicate(changes []model.Change) error
	LoadDevices() ([]*Device, error)
	CopyReplica(device *Device) error
	LoadEntry(entryId string) (*model.Entry, error)
	UpdateDeviceAuthToken(device *Device, authToken *AuthToken) error
}

type SqlitePersistentStorage struct {
	mu sync.RWMutex
	db *sql.DB
}

func (s *SqlitePersistentStorage) Initialize(workdir string) error {
	db, err := sql.Open("sqlite3", filepath.Join(workdir, "charlie.db"))
	if err != nil {
		return err
	}

	s.db = db

	createDeviceTableQuery := `
    CREATE TABLE IF NOT EXISTS devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid TEXT UNIQUE,
        deviceType TEXT,
        accessToken TEXT,
        accessTokenExpiration INTEGER,
        refreshToken TEXT,
        refreshTokenExpiration INTEGER
    )
    `

	createReplicaTableQuery := `
    CREATE TABLE IF NOT EXISTS replica (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entryUUID TEXT UNIQUE,
        deviceUUID TEXT,
        createdAt INTEGER,
        body BLOB
    )
    `

	createChangesTableQuery := `
	CREATE TABLE IF NOT EXISTS changes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		content BLOB  
	)
	`

	createChangesDevicesTableQuery := `
		CREATE TABLE IF NOT EXISTS changes_devices (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			changeId INTEGER,
			deviceId INTEGER,
			FOREIGN KEY(changeId) REFERENCES changes(id),
		    FOREIGN KEY(deviceId) REFERENCES devices(id)
		)
	`

	triggerSQL := `
    CREATE TRIGGER IF NOT EXISTS delete_unreferenced_changes
    AFTER DELETE ON changes_devices
    FOR EACH ROW
    WHEN NOT EXISTS (SELECT 1 FROM changes_devices WHERE changeId = OLD.changeId)
    BEGIN
        DELETE FROM changes WHERE id = OLD.changeId;
    END;
    `

	_, err = db.Exec("PRAGMA foreign_keys = ON;")
	if err != nil {
		return fmt.Errorf("error enabling foreign keys: %v", err)
	}

	if _, err = db.Exec(createDeviceTableQuery); err != nil {
		return fmt.Errorf("error creating device table: %v", err)
	}

	if _, err = db.Exec(createReplicaTableQuery); err != nil {
		return fmt.Errorf("error creating replica table: %v", err)
	}

	if _, err = db.Exec(createChangesTableQuery); err != nil {
		return fmt.Errorf("error creating changes table: %v", err)
	}

	if _, err = db.Exec(createChangesDevicesTableQuery); err != nil {
		return fmt.Errorf("error creating changes_devices table: %v", err)
	}

	_, err = db.Exec(triggerSQL)
	if err != nil {
		return fmt.Errorf("error executing trigger sql: %v", err)
	}

	return nil
}

func (s *SqlitePersistentStorage) LoadDevices() ([]*Device, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	selectDevicesQuery := `
	SELECT id, uuid, deviceType, accessToken, accessTokenExpiration, refreshToken, refreshTokenExpiration FROM devices;
	`

	rows, err := s.db.Query(selectDevicesQuery)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	devices := make([]*Device, 0, 2)

	for rows.Next() {
		var id int64
		var uuid string
		var deviceType string
		var accessToken string
		var accessTokenExpiration int64
		var refreshToken string
		var refreshTokenExpiration int64

		err = rows.Scan(&id, &uuid, &deviceType, &accessToken, &accessTokenExpiration, &refreshToken, &refreshTokenExpiration)
		if err != nil {
			return nil, err
		}

		authToken := &AuthToken{
			AccessToken:            accessToken,
			AccessTokenExpiration:  accessTokenExpiration,
			RefreshToken:           refreshToken,
			RefreshTokenExpiration: refreshTokenExpiration,
		}

		device, err := NewDevice(id, uuid, deviceType, authToken, 5)
		if err != nil {
			return nil, err
		}

		devices = append(devices, device)
	}

	return devices, nil
}

func (s *SqlitePersistentStorage) AddDevice(device *Device) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stmt, err := s.db.Prepare(`
        INSERT INTO devices (uuid, deviceType, accessToken, accessTokenExpiration, refreshToken, refreshTokenExpiration)
        VALUES (?, ?, ?, ?, ?, ?)
    `)

	if err != nil {
		return err
	}
	defer stmt.Close()

	res, err := stmt.Exec(device.UUID, device.DeviceType, device.AuthToken.AccessToken, device.AuthToken.AccessTokenExpiration, device.AuthToken.RefreshToken, device.AuthToken.RefreshTokenExpiration)
	if err != nil {
		return err
	}

	deviceSqliteId, err := res.LastInsertId()
	if err != nil {
		return err
	}

	device.Id = deviceSqliteId
	return nil
}

func (s *SqlitePersistentStorage) UpdateDeviceAuthToken(device *Device, authToken *AuthToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stmt, err := s.db.Prepare(`
        UPDATE devices 
        SET 
            AccessToken = ?, 
            AccessTokenExpiration = ?, 
            RefreshToken = ?, 
            RefreshTokenExpiration = ?
        WHERE id = ?;
    `)

	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(authToken.AccessToken, authToken.AccessTokenExpiration, authToken.RefreshToken, authToken.RefreshTokenExpiration, device.Id)
	if err != nil {
		return fmt.Errorf("error executing statement: %v", err)
	}

	return nil
}

func (s *SqlitePersistentStorage) LoadEntry(entryUUID string) (*model.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	selectEntryQuery := `
	SELECT entryUUID, deviceUUID, createdAt, body FROM replica WHERE entryUUID = ?;
	`

	stmt, err := s.db.Prepare(selectEntryQuery)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(entryUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entryUUID string
		var deviceUUID string
		var createdAt int64
		var body []byte

		if err := rows.Scan(&entryUUID, &deviceUUID, &createdAt, &body); err != nil {
			return nil, err
		}

		var result map[string]interface{}
		err := json.Unmarshal(body, &result)
		if err != nil {
			fmt.Println("Error unmarshalling JSON:", err)
			return nil, err
		}

		return &model.Entry{
			UUID:       entryUUID,
			DeviceUUID: deviceUUID,
			CreatedAt:  createdAt,
			Body:       result,
		}, nil
	}

	return nil, nil
}

func (s *SqlitePersistentStorage) RemoveDeliveredChanges(device *Device, changes []model.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stmt, err := s.db.Prepare(`
        DELETE FROM changes_devices 
        WHERE deviceId = ? AND changeId = ?
    `)

	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	defer stmt.Close()

	for _, change := range changes {
		_, err := stmt.Exec(device.Id, change.Id)
		if err != nil {
			return fmt.Errorf("failed to delete changes for device: %w", err)
		}
	}

	return nil
}

func (s *SqlitePersistentStorage) LoadUndeliveredChanges(device *Device, n int) ([]model.Change, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
        SELECT c.id, c.content 
        FROM changes_devices cd
        JOIN changes c ON cd.changeId = c.id
        WHERE cd.deviceId = ?
        LIMIT ?
    `

	rows, err := s.db.Query(query, device.Id, n)
	if err != nil {
		return nil, fmt.Errorf("failed to load undelivered changes: %w", err)
	}
	defer rows.Close()

	var changes []model.Change

	for rows.Next() {
		var id int64
		var content []byte

		err := rows.Scan(&id, &content)
		if err != nil {
			return nil, fmt.Errorf("failed to scan change row: %w", err)
		}

		var change model.Change
		if err := json.Unmarshal(content, &change); err != nil {
			return nil, err
		}

		change.Id = id
		changes = append(changes, change)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred during rows iteration: %w", err)
	}

	return changes, nil
}

func (s *SqlitePersistentStorage) PersistChangesForPeers(device *Device, peers []*Device, changes []model.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	insertChangeStmt, err := tx.Prepare(`
        INSERT INTO changes (content) 
        VALUES (?)
    `)

	if err != nil {
		return fmt.Errorf("failed to prepare change insert statement: %w", err)
	}

	defer insertChangeStmt.Close()

	insertChangesDevicesStmt, err := tx.Prepare(`
        INSERT INTO changes_devices (changeId, deviceId) 
        VALUES (?, ?)
    `)

	if err != nil {
		return fmt.Errorf("failed to prepare changes_devices insert statement: %w", err)
	}

	defer insertChangesDevicesStmt.Close()

	for i, change := range changes {
		serializedChange, err := json.Marshal(change)
		if err != nil {
			return err
		}

		result, err := insertChangeStmt.Exec(serializedChange)
		if err != nil {
			return fmt.Errorf("failed to persist change #%d: %w", i, err)
		}

		changeId, err := result.LastInsertId()
		if err != nil {
			return fmt.Errorf("failed to get change ID for change #%d: %w", i, err)
		}

		changes[i].Id = changeId

		for _, peer := range peers {
			if peer.DeviceType == "MIRROR" {
				continue
			}

			_, err := insertChangesDevicesStmt.Exec(changeId, peer.Id)
			if err != nil {
				return fmt.Errorf("failed to persist change for peer device %d: %w", peer.Id, err)
			}
		}
	}

	return err
}

func (s *SqlitePersistentStorage) Replicate(changes []model.Change) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	insertStmt, err := tx.Prepare(`
        	INSERT OR IGNORE INTO replica (entryUUID, deviceUUID, createdAt, body)
        	VALUES (?, ?, ?, ?)
    `)

	if err != nil {
		return err
	}
	defer insertStmt.Close()

	deleteStmt, err := tx.Prepare(`
		DELETE FROM replica WHERE entryUUID = ?
	`)

	if err != nil {
		return err
	}
	defer deleteStmt.Close()

	for _, change := range changes {
		if change.Operation == "ADD" {

			serializedBody, err := json.Marshal(change.Entry.Body)
			if err != nil {
				return err
			}

			if _, err := insertStmt.Exec(change.Entry.UUID, change.Entry.DeviceUUID, change.Entry.CreatedAt, serializedBody); err != nil {
				return err
			}

		} else if change.Operation == "REMOVE" {
			if _, err := deleteStmt.Exec(change.EntryUUID); err != nil {
				return err
			}
		}
	}

	return err
}

func (s *SqlitePersistentStorage) CopyReplica(device *Device) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	selectReplicaEntriesQuery := `SELECT entryUUID, deviceUUID, createdAt, body FROM replica`

	rows, err := s.db.Query(selectReplicaEntriesQuery)
	if err != nil {
		return nil
	}

	defer rows.Close()

	changes := make([][]byte, 0, 5)

	for rows.Next() {
		var entryUUID string
		var deviceUUID string
		var createdAt int64
		var body []byte

		err = rows.Scan(&entryUUID, &deviceUUID, &createdAt, &body)
		if err != nil {
			return err
		}

		var serializedBody map[string]interface{}
		if err := json.Unmarshal(body, &serializedBody); err != nil {
			return err
		}

		change := model.Change{
			Operation: "ADD",
			Entry: &model.Entry{
				UUID:       entryUUID,
				DeviceUUID: deviceUUID,
				CreatedAt:  createdAt,
				Body:       serializedBody,
			},
		}

		serializedChange, err := json.Marshal(change)
		if err != nil {
			return err
		}

		changes = append(changes, serializedChange)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	insertChangesQuery := `INSERT INTO changes (content) VALUES (?)`
	insertChangesDevicesQuery := `INSERT INTO changes_devices (changeId, deviceId) VALUES (?, ?)`

	insertChangesStmt, err := tx.Prepare(insertChangesQuery)
	if err != nil {
		return err
	}
	defer insertChangesStmt.Close()

	insertChangesDevicesStmt, err := tx.Prepare(insertChangesDevicesQuery)
	if err != nil {
		return err
	}
	defer insertChangesDevicesStmt.Close()

	for _, change := range changes {
		insertChangeResult, err := insertChangesStmt.Exec(change)
		if err != nil {
			return err
		}

		changeId, err := insertChangeResult.LastInsertId()
		if err != nil {
			return err
		}

		if _, err := insertChangesDevicesStmt.Exec(changeId, device.Id); err != nil {
			return err
		}
	}

	return err
}
