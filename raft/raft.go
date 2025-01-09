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
	prs := make(map[uint64]*Progress)
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Next: 1,
		}
	}

	r := &Raft{
		id:                  c.ID,
		RaftLog:             newLog(c.Storage),
		Prs:                 prs,
		State:               StateFollower,
		votes:               make(map[uint64]bool),
		msgs:                make([]pb.Message, 0),
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeoutBase: c.ElectionTick,
	}
	hardState, _, err := c.Storage.InitialState()
	if err == nil {
		r.Vote = hardState.Vote
		r.Term = hardState.Term
		r.RaftLog.committed = hardState.Commit
	}
	r.RaftLog.applied = c.Applied
	r.resetElectionTimeout()
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	progress := r.Prs[to]
	entries, err := r.RaftLog.Entries(progress.Next, r.RaftLog.LastIndex()+1)
	if err != nil {
		return false
	}
	ptrEntries := make([]*pb.Entry, 0, len(entries))
	for i := range entries {
		ptrEntries = append(ptrEntries, &entries[i])
	}
	prevLogTerm, err := r.RaftLog.Term(progress.Next - 1)
	if err != nil {
		log.Error(err)
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

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
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
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Lead = lead
	r.Term = term
	r.State = StateFollower
	r.electionElapsed = 0
	r.Vote = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term = r.Term + 1
	r.State = StateCandidate
	r.electionElapsed = 0
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
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
	r.State = StateLeader
	r.Lead = r.id
	lastIndex := r.RaftLog.LastIndex()
	for _, progess := range r.Prs {
		progess.Next = lastIndex + 1
		progess.Match = 0
	}
	r.Prs[r.id].Match = lastIndex

	r.handlePropose([]*pb.Entry{{}})
}

func (r *Raft) handleHup() {
	r.resetElectionTimeout()
	r.becomeCandidate()
	r.sendRequestVote()
}

func (r *Raft) handlePropose(entries []*pb.Entry) {
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
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgTransferLeader:
		case pb.MessageType_MsgTimeoutNow:
		default:
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleHup()
		case pb.MessageType_MsgBeat:
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgAppend:
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
		case pb.MessageType_MsgSnapshot:
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
			} else {
				r.Prs[m.From].Next = max(r.Prs[m.From].Match+1, r.getConflictIndex(m.LogTerm, m.Index))
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
			if r.RaftLog.LastIndex() > m.Index {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgTransferLeader:
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
		resp.LogTerm = r.RaftLog.entries[end].Term
		resp.Index = r.RaftLog.entries[end].Index
		//r.RaftLog.entries = r.RaftLog.entries[:end]
	} else {
		r.RaftLog.AppendEntries(m.Entries)
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries))) // handle empty Entries

		resp.Index, resp.LogTerm = r.RaftLog.LastIndexTerm()
	}
	r.msgs = append(r.msgs, resp)
}

func (r *Raft) getConflictIndex(conflictTerm, conflictIndex uint64) uint64 {
	end := uint64(len(r.RaftLog.entries) - 1)
	for ; end > 0 && r.RaftLog.entries[end].Term > conflictTerm; end-- {
	}
	for ; end > 0 && r.RaftLog.entries[end].Index > conflictIndex; end-- {
	}

	return r.RaftLog.entries[end].Index + 1
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
		Reject:  true,
	}
	if r.Term > m.Term {
		resp.Reject = true
	} else {
		resp.Index, resp.LogTerm = r.RaftLog.LastIndexTerm()

		r.Lead = m.From
		r.electionElapsed = 0

		committed := min(m.Commit, r.RaftLog.LastIndex())
		logTerm, err := r.RaftLog.Term(committed)
		if err == nil && m.Term == logTerm {
			r.RaftLog.committed = committed
		}
	}
	r.msgs = append(r.msgs, resp)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
