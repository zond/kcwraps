package subs

import (
	"bytes"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"time"
)

type Token struct {
	Principal string
	Timeout   time.Time
	Hash      []byte
	Encoded   string
}

var Secret = "something very secret"

func (self *Token) GetHash() (result []byte, err error) {
	h := sha512.New()
	if _, err = h.Write([]byte(fmt.Sprintf("%#v,%v,%#v", self.Principal, self.Timeout.UnixNano(), Secret))); err != nil {
		return
	}
	result = h.Sum(nil)
	return
}

func (self *Token) Encode() (err error) {
	self.Encoded = ""
	if self.Hash, err = self.GetHash(); err != nil {
		return
	}
	buf := &bytes.Buffer{}
	baseEnc := base64.NewEncoder(base64.URLEncoding, buf)
	gobEnc := gob.NewEncoder(base64.NewEncoder(base64.URLEncoding, buf))
	if err = gobEnc.Encode(self); err != nil {
		return
	}
	if err = baseEnc.Close(); err != nil {
		return
	}
	self.Encoded = buf.String()
	return
}

func DecodeToken(s string) (result *Token, err error) {
	dec := gob.NewDecoder(base64.NewDecoder(base64.URLEncoding, bytes.NewBufferString(s)))
	tok := &Token{}
	if err = dec.Decode(tok); err != nil {
		return
	}
	if tok.Timeout.After(time.Now()) {
		err = fmt.Errorf("Token %+v is timed out")
		return
	}
	correctHash, err := tok.GetHash()
	if err != nil {
		return
	}
	if len(tok.Hash) != len(correctHash) || subtle.ConstantTimeCompare(correctHash, tok.Hash) != 1 {
		err = fmt.Errorf("Token %+v has incorrect hash")
		return
	}
	result = tok
	return
}
