package main

import (
	"net"
)

type Oneport struct {
	root net.Listener
}

func New(l net.Listener) *Oneport {
	return &Oneport{
		root: l,
	}
}

func (p *Oneport) Serve() error {
	return nil
}
