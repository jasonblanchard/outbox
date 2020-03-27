package logger

import "log"

// Logger object holds log level state. level should be "debug" or "info"
type Logger struct {
	level string
}

// New returns a logger struct
func New(level string) Logger {
	return Logger{level}
}

// Info always prints the log
func (Logger) Info(s interface{}) {
	log.Print(s)
}

// Infof always prints the log
func (Logger) Infof(s string, a ...interface{}) {
	log.Printf(s, a...)
}

// Debug only prints if log level == debug
func (logger Logger) Debug(s interface{}) {
	if logger.level == "debug" {
		log.Print(s)
	}
}

// Debugf only prints if log level == debug
func (logger Logger) Debugf(s string, a ...interface{}) {
	if logger.level == "debug" {
		log.Printf(s, a...)
	}
}
