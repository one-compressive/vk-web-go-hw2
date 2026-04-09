package main

import (
	"cmp"
	"fmt"
	"log"
	"slices"
	"sync"
)

func logError(stage, format string, args ...any) {
	log.Printf("ERROR ["+stage+"] "+format, args...)
}

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
	// in - string
	// out - User
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	seen := make(map[uint64]struct{})

	for emailRaw := range in {
		email, ok := emailRaw.(string)
		if !ok {
			logError("SelectUsers", "wrong input type: want string, got %T (%v)", emailRaw, emailRaw)
			continue
		}

		wg.Add(1)
		go func(e string) {
			defer wg.Done()
			user := GetUser(e)
			isUnique := func() bool {
				mu.Lock()
				defer mu.Unlock()
				_, already := seen[user.ID]
				if !already {
					seen[user.ID] = struct{}{}
				}
				return !already
			}()
			if isUnique {
				out <- user
			}
		}(email)
	}
	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	// in - User
	// out - MsgID
	wg := &sync.WaitGroup{}
	batch := make([]User, 0, GetMessagesMaxUsersBatch)

	processBatch := func(b []User) {
		wg.Add(1)
		go func(users []User) {
			defer wg.Done()
			res, err := GetMessages(users...)
			if err != nil {
				logError("SelectMessages", "GetMessages failed: %v (users: %v)", err, users)
				return
			}
			for _, id := range res {
				out <- id
			}
		}(b)
	}

	for val := range in {
		user, ok := val.(User)
		if !ok {
			logError("SelectMessages", "wrong input type: want User, got %T", val)
			continue
		}
		batch = append(batch, user)
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
	// in - MsgID
	// out - MsgData
	wg := &sync.WaitGroup{}
	quota := make(chan struct{}, HasSpamMaxAsyncRequests)

	for msgIdRaw := range in {
		msgId, ok := msgIdRaw.(MsgID)
		if !ok {
			logError("CheckSpam", "wrong input type: want MsgID, got %T", msgIdRaw)
			continue
		}

		wg.Add(1)
		quota <- struct{}{}
		go func(id MsgID) {
			defer wg.Done()
			defer func() { <-quota }()
			isSpam, err := HasSpam(id)
			if err != nil {
				logError("CheckSpam", "HasSpam failed for id=%v: %v", id, err)
				return
			}
			out <- MsgData{ID: id, HasSpam: isSpam}
		}(msgId)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string
	var results []MsgData
	for val := range in {
		result, ok := val.(MsgData)
		if !ok {
			logError("CombineResults", "wrong input type: want MsgData, got %T (%v)", val, val)
			continue
		}
		results = append(results, result)
	}

	slices.SortFunc(results, func(a, b MsgData) int {
		if a.HasSpam != b.HasSpam {
			if a.HasSpam {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.ID, b.ID)
	})

	for _, r := range results {
		out <- fmt.Sprintf("%t %v", r.HasSpam, r.ID)
	}
}
