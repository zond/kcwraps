package subs

import (
	"github.com/zond/kcwraps/kol"
	"github.com/zond/wsubs/gosubs"
)

/*
Context describes a single WebSocket message and its environment
*/
type Context interface {
	gosubs.Context
	DB() *kol.DB
	Pack() *Pack
	BetweenTransactions(func(c Context))
	Transact(func(c Context) error) error
}

func NewContext(c gosubs.Context, pack *Pack, router *Router, db *kol.DB) Context {
	return &defaultContext{
		Context: c,
		pack:    pack,
		router:  router,
		db:      router.DB,
	}
}

type defaultContext struct {
	gosubs.Context
	pack   *Pack
	router *Router
	db     *kol.DB
}

func (self defaultContext) BetweenTransactions(f func(c Context)) {
	self.db.BetweenTransactions(func(d *kol.DB) {
		self.db = d
		f(&self)
	})
}

func (self defaultContext) Transact(f func(c Context) error) error {
	return self.db.Transact(func(d *kol.DB) error {
		self.db = d
		return f(&self)
	})
}

func (self *defaultContext) DB() *kol.DB {
	return self.db
}

func (self *defaultContext) Pack() *Pack {
	return self.pack
}

func (self *defaultContext) Router() *Router {
	return self.router
}
