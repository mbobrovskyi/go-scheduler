package goscheduler

type Logger interface {
	Debug(args ...any)
	Error(args ...any)
}

type nopLogger struct{}

func (n *nopLogger) Debug(args ...any) {}

func (n *nopLogger) Error(args ...any) {}

func newNopLogger() Logger {
	return &nopLogger{}
}
