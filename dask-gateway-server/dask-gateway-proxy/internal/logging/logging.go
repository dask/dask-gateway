// Our tiny custom logging framework.
//
// Provides common log levels, and quick functions for formatting and writing
// output at those levels.
package logging

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type LogLevel int8

const (
	ERROR LogLevel = iota - 1
	WARN
	INFO
	DEBUG
)

func ParseLevel(s string) LogLevel {
	switch s {
	case "ERROR", "error":
		return ERROR
	case "WARN", "warn":
		return WARN
	case "INFO", "info":
		return INFO
	case "DEBUG", "debug":
		return DEBUG
	}
	panic("Couldn't parse log level " + s)
}

func (l LogLevel) Char() byte {
	switch l {
	case ERROR:
		return 'E'
	case WARN:
		return 'W'
	case INFO:
		return 'I'
	case DEBUG:
		return 'D'
	}
	return '?'
}

type Logger struct {
	sync.Mutex
	Name  string
	Level LogLevel
	Out   io.Writer
	Buf   []byte
}

func NewLogger(name string, level LogLevel) *Logger {
	return &Logger{Name: name, Level: level, Out: os.Stderr}
}

func (l *Logger) logMsg(level LogLevel, msg string) {
	if l.Level >= level {
		now := time.Now() // get this early.
		l.Lock()
		defer l.Unlock()
		l.Buf = l.Buf[:0]
		l.Buf = append(l.Buf, '[')
		l.Buf = append(l.Buf, level.Char())
		l.Buf = append(l.Buf, ' ')
		l.Buf = now.AppendFormat(l.Buf, "2006-01-02 15:04:05.000")
		l.Buf = append(l.Buf, ' ')
		l.Buf = append(l.Buf, l.Name...)
		l.Buf = append(l.Buf, "] "...)
		l.Buf = append(l.Buf, msg...)
		l.Buf = append(l.Buf, '\n')
		l.Out.Write(l.Buf)
	}
}

func (l *Logger) logF(level LogLevel, format string, args ...interface{}) {
	if l.Level >= level {
		l.logMsg(level, fmt.Sprintf(format, args...))
	}
}

func (l *Logger) Debug(msg string) {
	l.logMsg(DEBUG, msg)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.logF(DEBUG, format, args...)
}

func (l *Logger) Info(msg string) {
	l.logMsg(INFO, msg)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.logF(INFO, format, args...)
}

func (l *Logger) Warn(msg string) {
	l.logMsg(WARN, msg)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.logF(WARN, format, args...)
}

func (l *Logger) Error(msg string) {
	l.logMsg(ERROR, msg)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.logF(ERROR, format, args...)
}
