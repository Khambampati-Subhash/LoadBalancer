package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"
)

const (
	walFile    = "wal.log"
	offsetFile = "offset.meta"
)

type Wal struct {
	Id        string    `json:"id"`
	AccountID uint64    `json:"accountID"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Data      int       `json:"data"`
	CreatedAt time.Time `json:"created_at"`
}

type Data struct {
	AccountID uint64 `json:"accountID"`
	Value     int    `json:"value"`
}

type OffsetMeta struct {
	LSN int64 `json:"lsn"`
}

/* ---------------- WAL APPEND ---------------- */

func appendWAL(entry *Wal) error {
	f, err := os.OpenFile(walFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}

	return f.Sync() // durability guarantee
}

/* ---------------- OFFSET READ ---------------- */

func readLSN() (int64, error) {
	f, err := os.Open(offsetFile)
	if err != nil {
		return 0, nil
	}
	defer f.Close()

	var meta OffsetMeta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return 0, nil
	}
	return meta.LSN, nil
}

func writeLSN(lsn int64) error {
	f, err := os.OpenFile(offsetFile, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	meta := OffsetMeta{LSN: lsn}
	if err := json.NewEncoder(f).Encode(&meta); err != nil {
		return err
	}

	return f.Sync() // commit marker
}

/* ---------------- WAL REPLAY (IDEMPOTENT) ---------------- */

func replayWAL(state map[string]*Data) (int64, error) {
	startLSN, _ := readLSN()

	f, err := os.Open(walFile)
	if err != nil {
		return startLSN, nil
	}
	defer f.Close()

	if _, err := f.Seek(startLSN, io.SeekStart); err != nil {
		return startLSN, err
	}

	reader := bufio.NewReader(f)
	currentLSN := startLSN

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return currentLSN, err
		}

		var e Wal
		if err := json.Unmarshal(line, &e); err != nil {
			break // partial WAL write → stop safely
		}

		from, ok1 := state[e.From]
		to, ok2 := state[e.To]
		if !ok1 || !ok2 {
			return currentLSN, errors.New("invalid account location")
		}

		if from.Value >= e.Data {
			from.Value -= e.Data
			to.Value += e.Data
		}

		currentLSN += int64(len(line))
	}

	return currentLSN, nil
}

/* ---------------- SNAPSHOT (FSYNC'D) ---------------- */

func persistSnapshot(state map[string]*Data) error {
	for name, data := range state {
		f, err := os.OpenFile(name+".log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}

		b, err := json.Marshal(data)
		if err != nil {
			f.Close()
			return err
		}

		if _, err := f.Write(b); err != nil {
			f.Close()
			return err
		}

		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}

		f.Close()
	}
	return nil
}

func main() {
	// Load snapshot
	load := func(file string) *Data {
		d := &Data{}
		b, err := os.ReadFile(file)
		if err == nil {
			json.Unmarshal(b, d)
		}
		return d
	}

	state := map[string]*Data{
		"bos":    load("bos.log"),
		"pune":   load("pune.log"),
		"london": load("london.log"),
	}

	// Example request
	entry := &Wal{
		Id:        "txn-001",
		AccountID: 123,
		From:      "bos",
		To:        "pune",
		Data:      3,
		CreatedAt: time.Now(),
	}

	// 1. WAL append (durable)
	if err := appendWAL(entry); err != nil {
		panic(err)
	}

	// 2. Replay WAL → memory
	lsn, err := replayWAL(state)
	if err != nil {
		panic(err)
	}

	// 3. Persist snapshot (fsync'd)
	if err := persistSnapshot(state); err != nil {
		panic(err)
	}

	// 4. Commit LSN AFTER snapshot
	if err := writeLSN(lsn); err != nil {
		panic(err)
	}
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
