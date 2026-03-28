package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	close(in)
	for _, c := range cmds {
		out := make(chan interface{})
		wg.Add(1)
		go func(c cmd, in, out chan interface{}) {
			defer wg.Done()
			defer close(out)
			c(in, out)
		}(c, in, out)
		in = out
	}
	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	seen := make(map[uint64]bool)

	for emailRaw := range in {
		email := emailRaw.(string)
		wg.Add(1)
		go func(e string) {
			defer wg.Done()
			user := GetUser(e)
			mu.Lock()
			isUnique := !seen[user.ID]
			if isUnique {
				seen[user.ID] = true
			}
			mu.Unlock()
			if isUnique {
				out <- user
			}
		}(email)
	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	batch := make([]User, 0, GetMessagesMaxUsersBatch)

	processBatch := func(b []User) {
		wg.Add(1)
		go func(users []User) {
			defer wg.Done()
			res, err := GetMessages(users...)
			if err == nil {
				for _, id := range res {
					out <- id
				}
			}
		}(b)
	}

	for val := range in {
		batch = append(batch, val.(User))
		if len(batch) == GetMessagesMaxUsersBatch {
			b := make([]User, len(batch))
			copy(b, batch)
			processBatch(b)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		processBatch(batch)
	}
	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	quota := make(chan struct{}, 5)

	for msgIdRaw := range in {
		msgId := msgIdRaw.(MsgID)
		wg.Add(1)
		quota <- struct{}{}
		go func(id MsgID) {
			defer wg.Done()
			defer func() { <-quota }()
			isSpam, err := HasSpam(id)
			if err == nil {
				out <- MsgData{ID: id, HasSpam: isSpam}
			}
		}(msgId)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var results []MsgData
	for val := range in {
		results = append(results, val.(MsgData))
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam != results[j].HasSpam {
			return results[i].HasSpam
		}
		return results[i].ID < results[j].ID
	})

	for _, r := range results {
		out <- fmt.Sprintf("%t %v", r.HasSpam, r.ID)
	}
}
