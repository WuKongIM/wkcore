package wkdb

import (
	"context"
	"errors"

	"github.com/cockroachdb/pebble"
)

type User struct {
	UID         string
	Token       string
	DeviceFlag  int64
	DeviceLevel int64
}

func (db *DB) CreateUser(ctx context.Context, u User) error {
	if err := db.checkContext(ctx); err != nil {
		return err
	}
	if err := validateUser(u); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	key := encodeUserPrimaryKey(u.UID, userPrimaryFamilyID)
	if _, err := db.getValue(key); err == nil {
		return ErrAlreadyExists
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	db.runAfterExistenceCheckHook()
	if err := db.checkContext(ctx); err != nil {
		return err
	}

	value := encodeUserFamilyValue(u.Token, u.DeviceFlag, u.DeviceLevel, key)

	batch := db.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(key, value, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (db *DB) GetUser(ctx context.Context, uid string) (User, error) {
	if err := db.checkContext(ctx); err != nil {
		return User{}, err
	}
	if uid == "" {
		return User{}, ErrInvalidArgument
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.getUserLocked(uid)
}

func (db *DB) getUserLocked(uid string) (User, error) {
	key := encodeUserPrimaryKey(uid, userPrimaryFamilyID)
	value, err := db.getValue(key)
	if err != nil {
		return User{}, err
	}

	token, deviceFlag, deviceLevel, err := decodeUserFamilyValue(key, value)
	if err != nil {
		return User{}, err
	}

	return User{
		UID:         uid,
		Token:       token,
		DeviceFlag:  deviceFlag,
		DeviceLevel: deviceLevel,
	}, nil
}

func (db *DB) UpdateUser(ctx context.Context, u User) error {
	if err := db.checkContext(ctx); err != nil {
		return err
	}
	if err := validateUser(u); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	key := encodeUserPrimaryKey(u.UID, userPrimaryFamilyID)
	if _, err := db.getValue(key); err != nil {
		return err
	}
	if err := db.checkContext(ctx); err != nil {
		return err
	}

	value := encodeUserFamilyValue(u.Token, u.DeviceFlag, u.DeviceLevel, key)

	batch := db.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(key, value, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (db *DB) DeleteUser(ctx context.Context, uid string) error {
	if err := db.checkContext(ctx); err != nil {
		return err
	}
	if uid == "" {
		return ErrInvalidArgument
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	key := encodeUserPrimaryKey(uid, userPrimaryFamilyID)
	if _, err := db.getValue(key); err != nil {
		return err
	}
	if err := db.checkContext(ctx); err != nil {
		return err
	}

	batch := db.db.NewBatch()
	defer batch.Close()

	if err := batch.Delete(key, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func validateUser(u User) error {
	if u.UID == "" || len(u.UID) > maxKeyStringLen {
		return ErrInvalidArgument
	}
	return nil
}
