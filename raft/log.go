// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	lastIndex, err2 := storage.LastIndex()
	entries, err3 := storage.Entries(firstIndex,lastIndex + 1)
	if err != nil || err2 != nil || err3 != nil{
		errors.New("l.storage.Entries() error!")
	}
	//stabled,_ := storage.Term(lastIndex)
	stabled := uint64(0)
	if len(entries) != 0{
		stabled = entries[len(entries) - 1].Index
	}
	return &RaftLog{
		storage: storage,
		committed: 0,
		applied: 0,
		//entries: append(make([]pb.Entry, 1),entries...) ,
		entries:entries,
		stabled: stabled,
	}

}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	for index,entry := range l.entries{
		if entry.Index > l.stabled{
			return l.entries[index:]
		}
	}
	return make([]pb.Entry, 0)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	
	
	appliedIndex := 0
	
	for index,entry := range l.entries{		
		if entry.Index == l.applied + 1{
			appliedIndex = index
		}
		if entry.Index == l.committed{
			return l.entries[appliedIndex : index + 1]
		}
	}
	return nil
}
// func (l *RaftLog) storeEnts() (ents []pb.Entry){
// 	firstIndex, err := l.storage.FirstIndex()
// 	lastIndex, err2 := l.storage.LastIndex()
// 	entries, err3 := l.storage.Entries(firstIndex,lastIndex + 1)
// 	if err != nil || err2 != nil || err3 != nil{
// 		errors.New("l.storage.Entries() error!")
// 	}
// 	return entries
// }

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0{
		return 0
	}
	return l.entries[len(l.entries) - 1].Index
	// entriesNum := len(l.entries)
	// if entriesNum != 0{
	// 	return l.entries[entriesNum - 1].Index
	// }
	// last, _ := l.storage.LastIndex()
	// lastEntry, err := 
	// if err != nil{
	// 	return 0
	// }
	// return lastEntry[0].Index
}

func (l *RaftLog) LastInfo() (uint64,uint64) {
	// Your Code Here (2A).
	if len(l.entries) == 0{
		return 0,0
	}
	e := l.entries[len(l.entries)-1]
	return e.Term,e.Index
	// entriesNum := len(l.entries)
	// if entriesNum != 0{
	// 	return l.entries[entriesNum - 1].Index
	// }
	// last, _ := l.storage.LastIndex()
	// lastEntry, err := 
	// if err != nil{
	// 	return 0
	// }
	// return lastEntry[0].Index
}



// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// ents := l.storeEnts()
	// for _,entry := range ents{
	// 	if entry.Index == i{
	// 		return entry.Term,nil
	// 	}
	// }
	if i == 0 {
		return 0,nil
	}
	for _,entry := range l.entries{
		if entry.Index == i{
			return entry.Term,nil
		}
	}
	
	
	return 0, errors.New("not found entry with the index")
}


func (l *RaftLog) getArrIndex(index,term uint64) int{
	for arrIndex,entry := range l.entries{
		if entry.Index == index && entry.Term == term{
			return arrIndex
		}
	}
	return -1
}

func (l *RaftLog) getArrIndexByIndex(index uint64) int{
	for arrIndex,entry := range l.entries{
		if entry.Index == index{
			return arrIndex
		}
	}
	return -1
}