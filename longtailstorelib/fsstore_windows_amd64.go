package longtailstorelib

import (
	"fmt"
	"syscall"
	"time"

	"github.com/pkg/errors"
)

type Lock struct {
	filename string
	handle   syscall.Handle
}

func NewFileLock(filename string) *Lock {
	return &Lock{filename: filename}
}

func (l *Lock) Lock() error {
	return l.LockWithTimeout(-1)
}

func (l *Lock) Unlock() error {
	err := syscall.Close(l.handle)
	if err != nil {
		return err
	}
	name, err := syscall.UTF16PtrFromString(l.filename)
	if err != nil {
		return err
	}
	// We don't care if we fail delete
	syscall.DeleteFile(name)
	return nil
}

func (l *Lock) LockWithTimeout(timeout time.Duration) (err error) {
	const fname = "Lock.LockWithTimeout"
	name, err := syscall.UTF16PtrFromString(l.filename)
	if err != nil {
		return errors.Wrap(err, fname)
	}

	l.handle, err = syscall.CreateFile(
		name,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0,
		nil,
		syscall.CREATE_ALWAYS,
		0,
		0)

	retry_delay := uint64(1000)

	start := time.Now()

	for l.handle == syscall.InvalidHandle {
		time.Sleep(time.Nanosecond * time.Duration(retry_delay))

		l.handle, err = syscall.CreateFile(
			name,
			syscall.GENERIC_READ|syscall.GENERIC_WRITE,
			0,
			nil,
			syscall.CREATE_ALWAYS,
			0,
			0)
		if err == nil {
			return nil
		}
		elapsed := time.Since(start)
		if timeout > 0 && elapsed > timeout {
			err := fmt.Errorf("Retry timed out for lock file %s, waited %s", l.filename, elapsed.String())
			return errors.Wrap(err, fname)
		}
		retry_delay += 2000
	}
	return errors.Wrap(err, fname)
}
