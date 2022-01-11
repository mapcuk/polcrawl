package main

import (
	"os"
	"testing"
)

func TestUnmarshall(t *testing.T) {
	bts, err := os.ReadFile("dmp/2.bin")
	err = parseMsg(bts)
	t.Log(err)
}
