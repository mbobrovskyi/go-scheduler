package logger

type LogLevel = string

const (
	PanicLevel LogLevel = "panic"
	ErrorLevel LogLevel = "error"
	WarnLevel  LogLevel = "warn"
	InfoLevel  LogLevel = "info"
	DebugLevel LogLevel = "debug"
)
