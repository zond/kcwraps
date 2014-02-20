package subs

import (
	"code.google.com/p/go.net/websocket"
	"github.com/zond/kcwraps/kol"
)

type defaultContext struct {
	conn      *websocket.Conn
	pack      *Pack
	message   *Message
	principal string
	match     []string
	data      JSON
	router    *Router
}

func (self *defaultContext) DB() *kol.DB {
	return self.router.DB
}

func (self *defaultContext) Conn() *websocket.Conn {
	return self.conn
}

func (self *defaultContext) Pack() *Pack {
	return self.pack
}

func (self *defaultContext) Message() *Message {
	return self.message
}

func (self *defaultContext) Principal() string {
	return self.principal
}

func (self *defaultContext) Match() []string {
	return self.match
}

func (self *defaultContext) Data() JSON {
	return self.data
}

func (self *defaultContext) Router() *Router {
	return self.router
}

func (self *defaultContext) Fatalf(format string, args ...interface{}) {
	self.router.Logf(Fatal, "\033[1;31mFATAL\t"+format+"\033[0m", args...)
}

func (self *defaultContext) Errorf(format string, args ...interface{}) {
	self.router.Logf(Error, "\033[31mERROR\t"+format+"\033[0m", args...)
}

func (self *defaultContext) Infof(format string, args ...interface{}) {
	self.router.Logf(Info, "INFO\t"+format, args...)
}

func (self *defaultContext) Debugf(format string, args ...interface{}) {
	self.router.Logf(Debug, "\033[32mDEBUG\t"+format+"\033[0m", args...)
}

func (self *defaultContext) Tracef(format string, args ...interface{}) {
	self.router.Logf(Trace, "\033[1;32mTRACE\t"+format+"\033[0m", args...)
}
