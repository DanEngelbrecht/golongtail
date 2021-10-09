// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

package longtailstorelib

import (
	"syscall"
	"time"
)

// Lock implements cross-process locks using syscalls.
// This implementation is based on flock syscall.
type Lock struct {
	filename string
	fd       int
}

// New returns a new lock around the given file.
func NewFileLock(filename string) *Lock {
	return &Lock{filename: filename}
}

// Lock locks the lock.  This call will block until the lock is available.
func (l *Lock) Lock() error {
	const fname = "Lock.open"
	if err := l.open(); err != nil {
		return errors.Wrap(err, fname)
	}
	return syscall.Flock(l.fd, syscall.LOCK_EX)
}

func (l *Lock) open() error {
	const fname = "Lock.open"
	l.fd, err := syscall.Open(l.filename, syscall.O_CREAT|syscall.O_RDONLY, 0600)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

// Unlock unlocks the lock.
func (l *Lock) Unlock() error {
	err := syscall.Close(l.fd)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

// LockWithTimeout tries to lock the lock until the timeout expires.  If the
// timeout expires, this method will return ErrTimeout.
func (l *Lock) LockWithTimeout(timeout time.Duration) error {
	const fname = "Lock.LockWithTimeout"
	l.fd, err := syscall.Open(l.filename, syscall.O_CREAT|syscall.O_RDONLY, 0600)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	result := make(chan error)
	cancel := make(chan struct{})
	go func() {
		err := syscall.Flock(l.fd, syscall.LOCK_EX)
		select {
		case <-cancel:
			// Timed out, cleanup if necessary.
			syscall.Flock(l.fd, syscall.LOCK_UN)
			syscall.Close(l.fd)
		case result <- err:
		}
	}()
	select {
	case err := <-result:
		return errors.Wrap(err, fname)
	case <-time.After(timeout):
		close(cancel)
		return ErrTimeout
	}
}
