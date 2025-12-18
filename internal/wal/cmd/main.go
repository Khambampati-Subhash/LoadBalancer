package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Wal struct {
	Id        string    `json:"id"`
	AccountID uint64    `json:"accountID"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Data      int       `json:"data"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

type Data struct {
	AccountID uint64 `json:"accountID"`
	Value     int    `json:"value"`
}

func appendWAL(entry *Wal) error {
	f, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	data, _ := json.Marshal(entry)
	_, err = f.Write(append(data, '\n'))
	if err != nil {
		return err
	}

	return f.Sync()
}

func replayWAL(state map[string]*Data) error {
	file, err := os.Open("wal.log")
	if err != nil {
		return nil // empty WAL is OK
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var e Wal
		json.Unmarshal(scanner.Bytes(), &e)

		if state[e.From].Value >= e.Data {
			state[e.From].Value -= e.Data
			state[e.To].Value += e.Data
		}
	}
	return nil
}

func main() {
	bos := &Data{}
	file1, err := os.ReadFile("bos.log")
	if err != nil {
		fmt.Errorf("Error while opening the file %s", err)
	}
	err = json.Unmarshal(file1, &bos)
	if err != nil {
		fmt.Errorf("Error while unmarshalling the data %s", err)
	}

	pune := &Data{}
	file2, err := os.ReadFile("pune.log")
	if err != nil {
		fmt.Errorf("Error while opening the file %s", err)
	}
	err = json.Unmarshal(file2, &pune)
	if err != nil {
		fmt.Errorf("Error while unmarshalling the data %s", err)
	}

	london := &Data{}
	file3, err := os.ReadFile("london.log")
	if err != nil {
		fmt.Errorf("Error while opening the file %s", err)
	}
	err = json.Unmarshal(file3, &london)
	if err != nil {
		fmt.Errorf("Error while unmarshalling the data %s", err)
	}
	fmt.Printf("Unmarshalled data properly %v, %v, %v", bos, pune, london)
	map1 := make(map[string]*Data, 0)

	// if _, exists := map1["bos"]; !exists {
	// 	map1["bos"] = new(Data)
	// }
	map1["bos"] = bos
	map1["pune"] = pune
	map1["london"] = london

	wal := &Wal{
		Id:        "iuhvnf0vfdskn9-ncks8-cnksd",
		AccountID: 123,
		From:      "bos",
		To:        "pune",
		Data:      3,
		CreatedAt: time.Now(),
	}
	err = appendWAL(wal)
	if err != nil {
		fmt.Errorf("While Appending wal error  %s", err)
	}

	err = replayWAL(map1)
	if err != nil {
		fmt.Errorf("While replaying the data  %s", err)
	}

	eew1, _ := json.Marshal(map1["bos"])
	os.WriteFile("bos.log", eew1, os.ModeAppend)

	eew2, _ := json.Marshal(map1["pune"])
	os.WriteFile("pune.log", eew2, os.ModeAppend)

	eew3, _ := json.Marshal(map1["london"])
	os.WriteFile("london.log", eew3, os.ModeAppend)
}

//
// ## The function (for reference)
//
// ```go
// func appendWAL(entry *Wal) error {
//     f, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
//     if err != nil {
//         return err
//     }
//     defer f.Close()
//
//     data, _ := json.Marshal(entry)
//     _, err = f.Write(append(data, '\n'))
//     if err != nil {
//         return err
//     }
//
//     return f.Sync()
// }
// ```
//
// ---
//
// # 1️⃣ `os.OpenFile(...)` — what exactly is happening?
//
// ```go
// os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
// ```
//
// This tells the OS **how** we want to open the file.
//
// ---
//
// ## 🔹 `os.O_WRONLY`
//
// > **Open file for write-only access**
//
// * You can **write** to the file
// * You cannot read from it
// * This matches WAL behavior perfectly:
//
//   * WAL is written sequentially
//   * WAL is never read during append
//
// Why not `O_RDWR`?
//
// * More permissions than needed
// * Violates least-privilege principle
// * Some OS-level optimizations depend on intent
//
// ---
//
// ## 🔹 `os.O_APPEND`
//
// > **Every write goes to the end of the file**
//
// This is **CRITICAL** for WAL.
//
// Without it:
//
// * Concurrent writes could overwrite data
// * File offsets could be corrupted
//
// With `O_APPEND`:
//
// * OS guarantees atomic append
// * Even with multiple goroutines/processes
//
// This is how databases safely allow concurrent writers.
//
// ---
//
// ## 🔹 `os.O_CREATE`
//
// > **Create the file if it doesn’t exist**
//
// Needed because:
//
// * On first startup, `wal.log` may not exist
// * Recovery should not fail due to missing WAL
//
// ---
//
// # 2️⃣ `0o644` — file permissions (Unix)
//
// ```go
// 0o644
// ```
//
// This is an **octal number**.
//
// ### Breakdown:
//
// | Digit | Meaning | Permission   |
// | ----- | ------- | ------------ |
// | `6`   | Owner   | read + write |
// | `4`   | Group   | read         |
// | `4`   | Others  | read         |
//
// So:
//
// ```text
// -rw-r--r--
// ```
//
// ### Why this is good for WAL
//
// * Process can write
// * Other users can inspect logs
// * No one else can modify WAL
//
// ---
//
// ### Why octal (`0o`)?
//
// * File permissions are bitmasks
// * Octal maps cleanly to permission bits
// * `0o644` is clearer than `0644` in modern Go
//
// ---
//
// # 3️⃣ `json.Marshal(entry)`
//
// ```go
// data, _ := json.Marshal(entry)
// ```
//
// * Converts your WAL struct → JSON bytes
// * Each WAL entry becomes a **self-contained record**
// * This is your **logical log record**
//
// Note:
//
// * Error ignored here for brevity
// * In production → always handle it
//
// ---
//
// # 4️⃣ `append(data, '\n')`
//
// ```go
// f.Write(append(data, '\n'))
// ```
//
// This creates **JSON Lines format**:
//
// ```text
// {...}
// {...}
// {...}
// ```
//
// Why newline?
//
// * Makes WAL **stream-friendly**
// * Enables line-by-line replay
// * Avoids parsing whole file into memory
//
// Databases and systems like Kafka do this.
//
// ---
//
// # 5️⃣ `f.Sync()` — THIS IS THE MOST IMPORTANT LINE
//
// ```go
// return f.Sync()
// ```
//
// ### What `Sync()` does
//
// > Forces the OS to flush file contents to **stable storage**
//
// Without `Sync()`:
//
// * OS may buffer writes in RAM
// * Power loss / crash → WAL entry lost
// * Client already got OK → data corruption
//
// ---
//
// ## 💥 What happens WITHOUT `fsync`
//
// Timeline:
//
// ```
// 1. f.Write() → OS buffer
// 2. return OK to client
// 3. crash / power loss
// 4. WAL entry gone
// ```
//
// 🔥 This violates WAL guarantees.
//
// ---
//
// ## ✅ What happens WITH `fsync`
//
// ```
// 1. f.Write()
// 2. f.Sync() → disk platter / SSD
// 3. return OK
// 4. crash → WAL survives
// ```
//
// ✔ This is why **databases are slow on writes**
// ✔ Durability always costs latency
//
// ---
//
// # 6️⃣ Why databases worship fsync
//
// | System     | Uses fsync? |
// | ---------- | ----------- |
// | PostgreSQL | YES         |
// | MySQL      | YES         |
// | Kafka      | YES         |
// | RocksDB    | YES         |
//
// If WAL is not fsync’d:
//
// > You don’t have a database, you have a cache.
//
// ---
//
// # 7️⃣ Why `defer f.Close()` is OK here
//
// * `Close()` releases file descriptor
// * Data is already flushed via `Sync()`
// * Safe even if crash happens after
//
// ---
//
// # 8️⃣ One-sentence summary (remember this)
//
// > **`os.O_APPEND` protects log order, `O_WRONLY` restricts access, `0644` controls permissions, and `fsync` makes WAL real.**
//
// ---
//
// # 9️⃣ Final mental model
//
// Think of WAL like:
//
// > ✍️ Writing in ink on paper — not pencil in memory.
//
// `fsync()` is the moment the ink dries.
//
// ---
//
// If you want, next we can:
//
// * Add **concurrent writers**
// * Add **transaction IDs**
// * Add **idempotent replay**
// * Simulate **crash mid-write**
//
// Just tell me the next step.
