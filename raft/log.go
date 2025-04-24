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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
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
	entries []pb.Entry // index start from 1, 0 is compacted

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
	if err != nil {
		panic(err)
	}
	snapIndex := firstIndex - 1
	snapTerm, err := storage.Term(snapIndex)
	if err != nil {
		panic(err)
	}

	stabledIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	var entries []pb.Entry
	stored, err := storage.Entries(firstIndex, stabledIndex+1)
	if err != nil {
		panic(err)
	}
	entries = make([]pb.Entry, 1, len(stored)+1)
	entries[0] = pb.Entry{Index: snapIndex, Term: snapTerm}
	entries = append(entries, stored...)

	return &RaftLog{
		storage: storage,
		stabled: stabledIndex,
		entries: entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return
	}
	snapIndex := firstIndex - 1
	if snapIndex > l.snapIndex() {
		l.entries = l.entries[snapIndex-l.snapIndex():]
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[l.stabled-l.snapIndex()+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	snapIndex := l.entries[0].Index
	if l.applied < l.committed {
		return l.entries[l.applied-snapIndex+1 : l.committed-snapIndex+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

func (l *RaftLog) LastIndexTerm() (index, term uint64) {
	lastEntry := l.entries[len(l.entries)-1]
	return lastEntry.Index, lastEntry.Term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	snapIndex := l.entries[0].Index
	if i < snapIndex {
		return 0, ErrCompacted
	} else if i > snapIndex+uint64(len(l.entries))-1 {
		return 0, ErrUnavailable
	}
	return l.entries[i-snapIndex].Term, nil
}

func (l *RaftLog) Entries(lo, hi uint64) ([]pb.Entry, error) {
	if lo == hi {
		return []pb.Entry{}, nil
	} else if lo > hi {
		return nil, ErrUnavailable
	}
	snapIndex := l.entries[0].Index
	if lo <= snapIndex {
		return nil, ErrCompacted
	} else if hi > snapIndex+uint64(len(l.entries)) {
		return nil, ErrUnavailable
	}
	return l.entries[lo-snapIndex : hi-snapIndex], nil
}

func (l *RaftLog) AppendEntries(entries []*pb.Entry) {
	for loc := 0; loc < len(entries); loc++ {
		logTerm, err := l.Term(entries[loc].Index)
		// Find first mismatch/notexist entry, then append two arrays
		if err != nil || logTerm != entries[loc].Term {
			stabledIndex := entries[loc].Index - 1
			l.stabled = min(l.stabled, stabledIndex)

			entries = entries[loc:]
			// If l.entries has more logs than entries, we won't remove them
			l.entries = l.entries[0 : stabledIndex-l.snapIndex()+1]
			for _, ent := range entries {
				l.entries = append(l.entries, *ent)
			}
			break
		}
	}
}

func (l *RaftLog) snapIndex() uint64 {
	return l.entries[0].Index
}
