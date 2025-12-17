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
	// for _, j := range eer {
	// 	if map1[j.From].Value >= j.Data {
	// 		map1[j.From].Value -= j.Data
	// 		map1[j.To].Value += j.Data
	// 		ee1, err := json.Marshal(map1[j.From])
	// 		if err != nil {
	// 			fmt.Printf("Error while marshalling the data %s", err)
	// 		}
	// 		ee2, err := json.Marshal(map1[j.To])
	// 		if err != nil {
	// 			fmt.Printf("Error while marshalling the data %s", err)
	// 		}
	// 		err = os.WriteFile(j.From+".json", ee1, os.ModeAppend)
	// 		if err != nil {
	// 			fmt.Printf("Error while writing the data %s", err)
	// 		}
	// 		err = os.WriteFile(j.To+".json", ee2, os.ModeAppend)
	// 		if err != nil {
	// 			fmt.Printf("Error while writing the data %s", err)
	// 		}
	// 	} else {
	// 		fmt.Errorf("Value in the %s is less than Data in the wal %v", j.From, j)
	// 	}
	// }
}
