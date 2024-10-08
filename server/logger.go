package server

import "log"

type Logger interface {
	Printf(format string, v ...any)
}

type DefaultLogger struct {
}

func NewDefaultLogger() (*DefaultLogger, error) {
	return &DefaultLogger{}, nil
}

func (l *DefaultLogger) Printf(format string, v ...any) {
	log.Printf(format, v...)
}

type NonLogger struct{}

func (l *NonLogger) Printf(format string, v ...any) {}

func NewNonLogger() *NonLogger {
	return &NonLogger{}
}
