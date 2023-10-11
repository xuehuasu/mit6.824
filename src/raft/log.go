package raft

import (
	"bytes"

	"6.824/labgob"
)

type Entry struct {
	Term    int
	Command interface{}
}

type Log struct {
	Entries []Entry
	Index0  int
}

func (log *Log) endIndex() int {
	return log.Index0 + len(log.Entries)
}

func (log *Log) offset() int {
	return log.Index0
}

func (log *Log) lastTerm() int {
	return log.Entries[len(log.Entries)-1].Term
}

func (log *Log) append(entry Entry) {
	log.Entries = append(log.Entries, entry)
}

func (log *Log) getEntry(index int) Entry {
	if (index < log.Index0) || (index >= log.endIndex()) {
		return Entry{-1, nil}
	}
	return log.Entries[index-log.Index0]
}

func (log *Log) setEntry(index int, entry Entry) {
	if index >= log.endIndex() {
		log.append(entry)
	} else {
		log.Entries[index-log.Index0] = entry
	}
}

// [:idx]
func (log *Log) cutEntryToIndex(index int) []Entry {
	return log.Entries[:index-log.Index0]
}

// [idx:]
func (log *Log) cutEntryToEnd(index int) []Entry {
	if index < log.Index0 {
		return log.Entries
	}
	return log.Entries[index-log.Index0:]
}

func (log *Log) getTerm(index int) int {
	if index < log.Index0 {
		return -1
	}
	if index >= log.endIndex() {
		return -2
	}
	return log.Entries[index-log.Index0].Term
}

func (log *Log) coding(entries []Entry) []byte {
	w := new(bytes.Buffer)
	for _, entry := range entries {
		e := labgob.NewEncoder(w)
		e.Encode(entry)
	}
	return w.Bytes()
}

func (log *Log) decoding(data []byte) []Entry {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var entries []Entry
	for {
		var entry Entry
		if d.Decode(&entry) != nil {
			break
		}
		entries = append(entries, entry)
	}
	return entries
}
