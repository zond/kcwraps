package subs

import (
	"github.com/zond/kcwraps/kol"
	"github.com/zond/wsubs/gosubs"
)

type defaultContext struct {
	gosubs.Context
	pack   *Pack
	router *Router
	db     *kol.DB
}

func (self *defaultContext) Transact(f func(c Context) error) error {
	return self.db.Transact(func(d *kol.DB) error {
		cpy := *self
		cpy.db = d
		return f(&cpy)
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
