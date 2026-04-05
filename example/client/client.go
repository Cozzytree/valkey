package main

import (
	"fmt"
	"log"

	valclient "github.com/valkey/valkey/internal/client"
)

func main() {
	c, err := valclient.Dial(":6379")
	if err != nil {
		log.Fatal(c)
	}
	defer func() {
		_ = c.Close()
	}()

	_ = c.Set("apple", "phone")
	if apple, err := c.Get("apple"); err != nil {
		return
	} else {
		fmt.Printf("KEY=apple, VALUE=%s\n", apple)
	}

	userid := "user:123"
	_, _ = c.HSet(userid, "name", "cozzytree")
	if value, err := c.HGet(userid, "name"); err != nil {
		return
	} else {
		fmt.Printf("HGET=%s, VALUE=%s", userid, value)
	}
}
