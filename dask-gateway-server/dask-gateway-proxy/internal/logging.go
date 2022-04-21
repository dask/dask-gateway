// Our tiny custom logging framework.
//
// Provides common log levels, and quick functions for formatting and writing
// output at those levels.
package main

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
	name  string
	level LogLevel
	out   io.Writer
	buf   []byte
}

func NewLogger(name string, level LogLevel) *Logger {
	return &Logger{name: name, level: level, out: os.Stderr}
}

func (l *Logger) logMsg(level LogLevel, msg string) {
	if l.level >= level {
		now := time.Now() // get this early.
		l.Lock()
		defer l.Unlock()
		l.buf = l.buf[:0]
		l.buf = append(l.buf, '[')
		l.buf = append(l.buf, level.Char())
		l.buf = append(l.buf, ' ')
		l.buf = now.AppendFormat(l.buf, "2006-01-02 15:04:05.000")
		l.buf = append(l.buf, ' ')
		l.buf = append(l.buf, l.name...)
		l.buf = append(l.buf, "] "...)
		l.buf = append(l.buf, msg...)
		l.buf = append(l.buf, '\n')
		l.out.Write(l.buf)
	}
}

func (l *Logger) logF(level LogLevel, format string, args ...interface{}) {
	if l.level >= level {
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
