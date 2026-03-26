package wkdb

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestOpenClose(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func TestErrorsExposeStableSentinels(t *testing.T) {
	if !errors.Is(ErrNotFound, ErrNotFound) {
		t.Fatal("ErrNotFound should match itself")
	}
	if !errors.Is(ErrAlreadyExists, ErrAlreadyExists) {
		t.Fatal("ErrAlreadyExists should match itself")
	}
}

func TestUserCRUD(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db, err := Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})

	_, err = db.GetUser(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing user, got %v", err)
	}

	created := User{
		UID:         "u1001",
		Token:       "tk_abc",
		DeviceFlag:  1,
		DeviceLevel: 2,
	}
	if err := db.CreateUser(ctx, created); err != nil {
		t.Fatalf("create user: %v", err)
	}

	if err := db.CreateUser(ctx, created); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	got, err := db.GetUser(ctx, created.UID)
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if !reflect.DeepEqual(got, created) {
		t.Fatalf("unexpected user:\n got: %#v\nwant: %#v", got, created)
	}

	updated := User{
		UID:         "u1001",
		Token:       "tk_new",
		DeviceFlag:  5,
		DeviceLevel: 9,
	}
	if err := db.UpdateUser(ctx, updated); err != nil {
		t.Fatalf("update user: %v", err)
	}

	got, err = db.GetUser(ctx, updated.UID)
	if err != nil {
		t.Fatalf("get updated user: %v", err)
	}
	if !reflect.DeepEqual(got, updated) {
		t.Fatalf("unexpected updated user:\n got: %#v\nwant: %#v", got, updated)
	}

	if err := db.DeleteUser(ctx, updated.UID); err != nil {
		t.Fatalf("delete user: %v", err)
	}

	_, err = db.GetUser(ctx, updated.UID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestCreateUserConcurrentDuplicateReturnsErrAlreadyExists(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db, err := Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})

	blocked := make(chan struct{})
	reached := make(chan struct{})
	db.testHooks.afterExistenceCheck = func() {
		close(reached)
		<-blocked
	}

	firstErr := make(chan error, 1)
	secondErr := make(chan error, 1)

	go func() {
		firstErr <- db.CreateUser(ctx, User{UID: "u2001"})
	}()

	<-reached

	go func() {
		secondErr <- db.CreateUser(ctx, User{UID: "u2001"})
	}()

	select {
	case err := <-secondErr:
		t.Fatalf("second create returned before first commit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(blocked)

	if err := <-firstErr; err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := <-secondErr; !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestCreateUserRejectsOverlongUID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db, err := Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})

	u := User{UID: strings.Repeat("a", maxKeyStringLen+1)}
	err = db.CreateUser(ctx, u)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestGetUserHonorsCanceledContext(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = db.GetUser(ctx, "u1001")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
