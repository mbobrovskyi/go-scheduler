package logger

import "io"

type NopLogger struct{}

func (n NopLogger) Debug(args ...any) {}

func (n NopLogger) Debugf(format string, args ...any) {}

func (n NopLogger) Info(args ...any) {}

func (n NopLogger) Infof(format string, args ...any) {}

func (n NopLogger) Warn(args ...any) {}

func (n NopLogger) Warnf(format string, args ...any) {}

func (n NopLogger) Error(args ...any) {}

func (n NopLogger) Errorf(format string, args ...any) {}

func (n NopLogger) Fatal(args ...any) {}

func (n NopLogger) Fatalf(format string, args ...any) {}

func (n NopLogger) Writer() *io.PipeWriter {
	return nil
}

func NewNopLogger() *NopLogger {
	return &NopLogger{}
}
