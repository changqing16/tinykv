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
	"math/rand"
	"slices"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// Tag which is useful for printing log
	Tag string

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64
	// Tag which is useful for printing log
	Tag string

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeoutBase int
	electionTimeout     int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	leadTransferElapsed int

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	r := &Raft{
		id:                  c.ID,
		Tag:                 c.Tag,
		RaftLog:             newLog(c.Storage),
		State:               StateFollower,
		votes:               make(map[uint64]bool),
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeoutBase: c.ElectionTick,
	}
	r.RaftLog.applied = max(c.Applied, r.RaftLog.snapIndex())

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r.Vote = hardState.Vote
	r.Term = hardState.Term
	r.RaftLog.committed = max(hardState.Commit, r.RaftLog.applied)

	next := r.RaftLog.LastIndex() + 1
	prs := make(map[uint64]*Progress)
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Next: next,
		}
	}
	for _, peer := range confState.Nodes {
		prs[peer] = &Progress{
			Next: next,
		}
	}
	r.Prs = prs

	r.resetElectionTimeout()
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	progress := r.Prs[to]
	if progress.Next <= r.RaftLog.snapIndex() {
		r.sendSnapshot(to)
		return true
	}
	// can send empty entries to update peer Commit and Next index
	entries, err := r.RaftLog.Entries(progress.Next, r.RaftLog.LastIndex()+1)
	if err != nil {
		log.Errorf("%s, get sendAppend entires failed, progress.Next: %d, lastIndex+1: %d, err: %v",
			r.Tag, progress.Next, r.RaftLog.LastIndex()+1, err)
		return false
	}
	ptrEntries := make([]*pb.Entry, 0, len(entries))
	for i := range entries {
		ptrEntries = append(ptrEntries, &entries[i])
	}
	prevLogTerm, err := r.RaftLog.Term(progress.Next - 1)
	if err != nil {
		log.Errorf("%s, get prevLogTerm failed, progress.Next-1: %d, lastIndex: %d, err: %v",
			r.Tag, progress.Next-1, r.RaftLog.LastIndex(), err)
		return false
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   progress.Next - 1,
		Entries: ptrEntries,
		Commit:  r.RaftLog.committed,
	})

	return true
}

func (r *Raft) sendSnapshot(to uint64) {
	var snapShot pb.Snapshot
	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		snapShot = *r.RaftLog.pendingSnapshot
	} else {
		var err error
		snapShot, err = r.RaftLog.storage.Snapshot()
		if err != nil {
			return
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapShot,
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) resetElectionTimeout() {
	r.electionTimeout = r.electionTimeoutBase + rand.Intn(r.electionTimeoutBase)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout {
			r.handleHup()
		}
	case StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout {
			r.handleHup()
		}
	case StateLeader:
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.handleBeat()
		}
		if r.leadTransferee != 0 {
			r.leadTransferElapsed += 1
			if r.leadTransferElapsed >= r.electionTimeout {
				r.leadTransferee = 0
				r.leadTransferElapsed = 0
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Lead = lead
	r.Term = term
	r.State = StateFollower
	r.Vote = 0
	r.leadTransferee = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term = r.Term + 1
	r.State = StateCandidate
	r.Vote = r.id
	r.electionElapsed = 0
	r.resetElectionTimeout()
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.leadTransferee = 0
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) sendRequestVote() {
	lastIndex, lastTerm := r.RaftLog.LastIndexTerm()
	for peer := range r.Prs {
		if _, ok := r.votes[peer]; !ok {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peer,
				From:    r.id,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastTerm,
			})
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	peers := make([]uint64, 0, len(r.Prs))
	for key := range r.Prs {
		peers = append(peers, key)
	}
	slices.Sort(peers)
	log.Infof("%s becomeLeader, term:%d, peers:%v", r.Tag, r.Term, peers)
	r.State = StateLeader
	r.Lead = r.id
	lastIndex := r.RaftLog.LastIndex()
	for _, progess := range r.Prs {
		progess.Next = lastIndex + 1
		progess.Match = 0
	}
	r.Prs[r.id].Match = lastIndex
	r.PendingConfIndex = r.getLatestPendingConfIndex()
	r.handlePropose([]*pb.Entry{{}})
}

func (r *Raft) handleHup() {
	r.becomeCandidate()
	r.sendRequestVote()
}

func (r *Raft) handlePropose(entries []*pb.Entry) {
	if r.leadTransferee != 0 {
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		lastIndex += 1
		entries[i].Index = lastIndex
		r.RaftLog.entries = append(r.RaftLog.entries, *entries[i])
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) handleBeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
	r.heartbeatElapsed = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if r.Term < m.Term {
		r.becomeFollower(m.Term, 0)
		return r.Step(m)
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup()
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgTransferLeader,
				From:    r.id,
				To:      r.Lead,
			})
		case pb.MessageType_MsgTimeoutNow:
			if r.Prs[r.id] == nil {
				return nil
			}
			return r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		default:
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup()
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend, pb.MessageType_MsgSnapshot:
			if r.Term == m.Term {
				r.becomeFollower(m.Term, m.From)
				return r.Step(m)
			} else if r.Term > m.Term {
				resp := pb.Message{
					MsgType: pb.MessageType_MsgAppendResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				}
				r.msgs = append(r.msgs, resp)
			}
		case pb.MessageType_MsgAppendResponse:
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			var voted, rejected int
			for peer := range r.Prs {
				if _, ok := r.votes[peer]; ok {
					if r.votes[peer] {
						voted += 1
					} else {
						rejected += 1
					}
				}
			}
			if voted > len(r.Prs)/2 {
				r.becomeLeader()
			}
			if rejected > len(r.Prs)/2 {
				r.becomeFollower(m.Term, 0)
			}
		case pb.MessageType_MsgHeartbeat:
			if r.Term == m.Term {
				r.becomeFollower(m.Term, m.From)
				return r.Step(m)
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		default:
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
		case pb.MessageType_MsgBeat:
			r.handleBeat()
		case pb.MessageType_MsgPropose:
			r.handlePropose(m.Entries)
		case pb.MessageType_MsgAppend:
		case pb.MessageType_MsgAppendResponse:
			if !m.Reject {
				r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
				r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)
				r.updateCommitted()
				if r.leadTransferee == m.From && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
					r.leadTransferee = 0
					r.msgs = append(r.msgs, pb.Message{
						MsgType: pb.MessageType_MsgTimeoutNow,
						From:    r.id,
						To:      m.From,
						Term:    r.Term,
					})
				}
			} else {
				r.Prs[m.From].Next = max(r.Prs[m.From].Match+1, r.getConflictNextIndex(m.LogTerm, m.Index))
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		case pb.MessageType_MsgRequestVoteResponse:
		case pb.MessageType_MsgSnapshot:
		case pb.MessageType_MsgHeartbeat:
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
			})
		case pb.MessageType_MsgHeartbeatResponse:
			if r.RaftLog.LastIndex() > m.Commit {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgTransferLeader:
			if m.From == r.id || r.Prs[m.From] == nil {
				return nil
			}
			r.leadTransferee = m.From
			if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgTimeoutNow,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
				})
			} else {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgTimeoutNow:
		default:
		}
	}
	return nil
}

func (r *Raft) updateCommitted() {
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}

	majority := len(r.Prs) / 2

	committed := r.RaftLog.committed
	updateCommit := true
	for updateCommit {
		updateCommit = false
		count := 0
		for _, progress := range r.Prs {
			if progress.Match > committed {
				count++
				if count > majority {
					updateCommit = true
					committed++
					break
				}
			}
		}
	}
	//endTerm, _ := rf.getLogTermIndex(endIndex)
	//rf.logs[0]的Index应当小于等于commitIndex
	logTerm, _ := r.RaftLog.Term(committed)

	if logTerm == r.Term && r.RaftLog.committed != committed {
		r.RaftLog.committed = committed
		for peer := range r.Prs {
			if peer == r.id {
				continue
			}
			r.sendAppend(peer)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	if r.Term > m.Term {
		resp.Reject = true
		r.msgs = append(r.msgs, resp)
		return
	}
	r.Lead = m.From

	logTerm, err := r.RaftLog.Term(m.Index)
	if err != nil || logTerm != m.LogTerm {
		resp.Reject = true

		end := len(r.RaftLog.entries) - 1
		for ; end > 0 && r.RaftLog.entries[end].Term > m.LogTerm; end-- {
		}
		for ; end > 0 && r.RaftLog.entries[end].Index > m.Index; end-- {
		}
		entry := r.RaftLog.entries[end]
		resp.Index, resp.LogTerm = entry.Index, entry.Term
	} else {
		r.RaftLog.AppendEntries(m.Entries)
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries))) // handle empty Entries

		resp.Index, resp.LogTerm = r.RaftLog.LastIndexTerm()
	}
	r.msgs = append(r.msgs, resp)
}

func (r *Raft) getConflictNextIndex(conflictTerm, conflictIndex uint64) uint64 {
	end := len(r.RaftLog.entries) - 1
	for ; end >= 0 && r.RaftLog.entries[end].Term > conflictTerm; end-- {
	}
	for ; end >= 0 && r.RaftLog.entries[end].Index > conflictIndex; end-- {
	}
	if end == len(r.RaftLog.entries)-1 {
		return r.RaftLog.entries[end].Index + 1
	}
	return r.RaftLog.entries[end+1].Index
}

func (r *Raft) handleRequestVote(m pb.Message) {
	lastIndex, lastTerm := r.RaftLog.LastIndexTerm()
	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   lastIndex,
	}
	if r.Term > m.Term {
		resp.Reject = true
	} else if r.Vote != 0 {
		resp.Reject = (r.Vote != m.From)
	} else if lastTerm < m.LogTerm || (lastTerm == m.LogTerm && lastIndex <= m.Index) {
		resp.Reject = false
		r.Vote = m.From
		r.electionElapsed = 0
	} else {
		resp.Reject = true
	}
	r.msgs = append(r.msgs, resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	if r.Term > m.Term {
		resp.Reject = true
	} else {
		r.Lead = m.From
		r.electionElapsed = 0
		resp.Commit = r.RaftLog.committed
	}
	r.msgs = append(r.msgs, resp)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	if r.Term > m.Term {
		resp.Reject = true
		r.msgs = append(r.msgs, resp)
		return
	}
	r.Lead = m.From

	metaData := m.Snapshot.GetMetadata()
	newSnapIndex := metaData.GetIndex()
	if newSnapIndex <= r.RaftLog.committed {
		resp.Reject = true
		resp.Index = r.RaftLog.committed
		resp.LogTerm, _ = r.RaftLog.Term(r.RaftLog.committed)
	} else {
		resp.Index, resp.LogTerm = metaData.GetIndex(), metaData.GetTerm()

		if newSnapIndex-r.RaftLog.snapIndex() >= uint64(len(r.RaftLog.entries)) {
			r.RaftLog.entries = []pb.Entry{
				{
					EntryType: pb.EntryType_EntryNormal,
					Term:      metaData.GetTerm(),
					Index:     newSnapIndex,
				},
			}
		} else {
			r.RaftLog.entries = r.RaftLog.entries[newSnapIndex-r.RaftLog.snapIndex():]
		}

		r.RaftLog.committed = newSnapIndex
		r.RaftLog.applied = newSnapIndex
		r.RaftLog.stabled = newSnapIndex

		r.Prs = make(map[uint64]*Progress)
		for _, peer := range metaData.ConfState.Nodes {
			r.Prs[peer] = &Progress{
				Next: r.RaftLog.LastIndex() + 1,
			}
		}
		r.RaftLog.pendingSnapshot = m.Snapshot
	}
	r.msgs = append(r.msgs, resp)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{
		Next: r.RaftLog.LastIndex() + 1,
	}
	r.PendingConfIndex = 0
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	r.updateCommitted()
	r.PendingConfIndex = 0
}

func (r *Raft) PendingConfChange() bool {
	return r.PendingConfIndex > r.RaftLog.applied
}

func (r *Raft) getLatestPendingConfIndex() uint64 {
	for i := len(r.RaftLog.entries) - 1; i > 0 && r.RaftLog.entries[i].Index > r.RaftLog.applied; i-- {
		if r.RaftLog.entries[i].EntryType == pb.EntryType_EntryConfChange {
			return r.RaftLog.entries[i].Index
		}
	}
	return 0
}

func (r *Raft) HardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) SoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}
