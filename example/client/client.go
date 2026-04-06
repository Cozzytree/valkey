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
		fmt.Printf("HGET=%s, VALUE=%s\n", userid, value)
	}

	err = c.JSONSet("userAI", "$", "asdas")
	fmt.Printf("jsonset error: %v\n", err)

	if res, err := c.JSONGet("userAI"); err != nil {
		fmt.Printf("error json response: %v\n", err)
	} else {
		fmt.Printf("json set response: %v\n", res)
	}
}
