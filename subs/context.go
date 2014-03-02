package subs

import (
	"github.com/zond/kcwraps/kol"
	"github.com/zond/wsubs/gosubs"
)

type defaultContext struct {
	gosubs.Context
	pack   *Pack
	router *Router
}

func (self *defaultContext) DB() *kol.DB {
	return self.router.DB
}

func (self *defaultContext) Pack() *Pack {
	return self.pack
}

func (self *defaultContext) Router() *Router {
	return self.router
}
