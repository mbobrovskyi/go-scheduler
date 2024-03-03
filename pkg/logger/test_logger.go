package logger

import (
	"io"
	"testing"
)

type TestLogger struct {
	t *testing.T
}

func (l TestLogger) Debug(args ...any) {
	l.t.Log(args...)
}

func (l TestLogger) Debugf(format string, args ...any) {
	l.t.Logf(format, args...)
}

func (l TestLogger) Info(args ...any) {
	l.t.Log(args...)
}

func (l TestLogger) Infof(format string, args ...any) {
	l.t.Logf(format, args...)
}

func (l TestLogger) Warn(args ...any) {
	l.t.Log(args...)
}

func (l TestLogger) Warnf(format string, args ...any) {
	l.t.Logf(format, args...)
}

func (l TestLogger) Error(args ...any) {
	l.t.Log(args...)
}

func (l TestLogger) Errorf(format string, args ...any) {
	l.t.Logf(format, args...)
}

func (l TestLogger) Fatal(args ...any) {
	l.t.Log(args...)
}

func (l TestLogger) Fatalf(format string, args ...any) {
	l.t.Logf(format, args...)
}

func (l TestLogger) Writer() *io.PipeWriter {
	return nil
}

func NewTestLogger(t *testing.T) Logger {
	return &TestLogger{
		t: t,
	}
}
