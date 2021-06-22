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
	"log"
	"math/rand"
	"os"
)

// Debugging
const Debug = 1

var myLog = log.New(os.Stderr, "", log.LstdFlags)

func init() {
	file, _ := os.OpenFile("/home/qing/logs/2.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	myLog.SetOutput(file)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		myLog.Printf(format, a...)
	}
	return
}

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
	electionTimeout       int
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
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
	rf := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		State:            StateFollower,
	}

	st, cf, _ := rf.RaftLog.storage.InitialState()
	if c.peers == nil {
		c.peers = cf.Nodes
	}
	rf.Term = st.Term
	rf.Vote = st.Vote
	rf.RaftLog.committed = st.Commit
	rf.RaftLog.applied = c.Applied

	lastIndex := rf.RaftLog.LastIndex()

	DPrintf("%d, start with term: %d, %d entries, lastIndex:%d", rf.id, rf.Term, len(rf.RaftLog.entries), lastIndex)

	rf.Prs = make(map[uint64]*Progress)
	for _, peer := range c.peers {
		rf.Prs[peer] = &Progress{
			Match: 0,
			Next:  lastIndex + 1,
		}
	}

	rf.randomElectionTimeout = rf.electionTimeout + rand.Intn(rf.electionTimeout)
	return rf
}

// tick advances the internal logical clock by a single tick.
func (rf *Raft) tick() {
	// Your Code Here (2A).
	if rf.State == StateLeader {
		rf.tickLeader()
	} else {
		rf.tickElection()
	}
}

func (rf *Raft) tickLeader() {
	rf.bcastBeat()
}

func (rf *Raft) tickElection() {
	rf.electionElapsed++
	if rf.electionElapsed > rf.randomElectionTimeout {
		rf.becomeCandidate()
		rf.bcastRequestVote()
	}
}

// becomeFollower transform this peer's state to Follower
func (rf *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	rf.Term = term
	rf.Vote = None
	rf.Lead = lead
	rf.State = StateFollower
	DPrintf("%d, become follower, term: %d, lead: %d", rf.id, rf.Term, rf.Lead)
}

// becomeCandidate transform this peer's state to candidate
func (rf *Raft) becomeCandidate() {
	// Your Code Here (2A).
	rf.randomElectionTimeout = rf.electionTimeout + rand.Intn(rf.electionTimeout)
	rf.electionElapsed = 0
	rf.Term++
	rf.State = StateCandidate
	rf.Vote = rf.id
	rf.votes = make(map[uint64]bool)
	rf.votes[rf.id] = true

	DPrintf("%d, become candidate, term: %d", rf.id, rf.Term)
}

// becomeLeader transform this peer's state to leader
func (rf *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	DPrintf("%d, become leader, term:%d", rf.id, rf.Term)
	rf.State = StateLeader
	rf.Lead = rf.id

	lastIndex := rf.RaftLog.LastIndex()
	for _, v := range rf.Prs {
		v.Next = lastIndex + 1
		v.Match = 0
	}

	//make noop propose
	entries := make([]*pb.Entry, 1, 1)
	entries[0] = &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
	}
	rf.appendPropose(entries)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (rf *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	DPrintf("%d, %v, receive %v from %d with term: %d", rf.id, rf.State, m.MsgType, m.From, m.Term)
	switch rf.State {
	case StateFollower:
		return rf.stepFollower(m)
	case StateCandidate:
		return rf.stepCandidate(m)
	case StateLeader:
		return rf.stepLeader(m)
	}
	return nil
}

func (rf *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		rf.handleMsgHup()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		rf.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		rf.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		rf.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (rf *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		rf.handleMsgHup()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		rf.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		if m.Term > rf.Term {
			rf.handleRequestVote(m)
		} else {
			reply := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    rf.id,
				Term:    rf.Term,
				Reject:  true,
			}
			rf.msgs = append(rf.msgs, reply)
		}
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term > rf.Term {
			rf.becomeFollower(m.Term, None)
		} else if m.Term == rf.Term { //Term相等说明消息没有过时
			rf.votes[m.From] = !m.Reject
			rf.countVote()
		}
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		rf.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (rf *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		rf.bcastBeat()
	case pb.MessageType_MsgPropose:
		rf.appendPropose(m.Entries)
	case pb.MessageType_MsgAppend:
		if m.Term > rf.Term {
			rf.handleAppendEntries(m)
		} else {
			reply := pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    rf.id,
				Term:    rf.Term,
				Reject:  true, //
			}
			rf.msgs = append(rf.msgs, reply)
		}
	case pb.MessageType_MsgAppendResponse:
		if m.Term == rf.Term {
			if m.Reject == false {
				rf.Prs[m.From].Match = max(rf.Prs[m.From].Match, m.Index)
				rf.Prs[m.From].Next = max(rf.Prs[m.From].Next, m.Index+1)
				if rf.updateCommit() { //太蠢了吧，有好好的heartbeat不用
					rf.bcastAppend()
				}
			} else {
				rf.Prs[m.From].Next = max(rf.Prs[m.From].Match+1, rf.getConflictIndex(m.LogTerm, m.Index))
				rf.sendAppend(m.From)
			}
		} else if m.Term > rf.Term {
			rf.becomeFollower(m.Term, None)
		}
		//如果返回的Term小于当前的Term说明这个消息是过期的，直接丢弃
	case pb.MessageType_MsgRequestVote:
		if m.Term > rf.Term { //
			rf.handleRequestVote(m)
		} else {
			reply := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    rf.id,
				Term:    rf.Term,
				Reject:  true,
			}
			rf.msgs = append(rf.msgs, reply)
		}
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term > rf.Term {
			rf.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgSnapshot:

	case pb.MessageType_MsgHeartbeat:
		rf.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Term > rf.Term {
			rf.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (rf *Raft) handleMsgHup() {
	rf.becomeCandidate()
	rf.bcastRequestVote()
}

func (rf *Raft) bcastRequestVote() {
	// when term change, the votes need to be reset
	//send RequestVote to other peer
	DPrintf("%d, broadcast RequestVote", rf.id)

	if len(rf.Prs) == 1 {
		rf.becomeLeader()
		return
	}

	index, term := rf.RaftLog.LastIndexTerm()
	for peer := range rf.Prs {
		if peer != rf.id {
			rf.msgs = append(rf.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peer,
				From:    rf.id,
				Term:    rf.Term,
				LogTerm: term,
				Index:   index,
			})
		}
	}
}

func (rf *Raft) handleRequestVote(m pb.Message) {
	lastIndex, lastTerm := rf.RaftLog.LastIndexTerm()
	DPrintf("lastTerm: %d, lastIndex: %d, vote: %d, lastTerm: %d, lastIndex: %d",
		lastTerm, lastIndex, rf.Vote, m.LogTerm, m.Index)

	reply := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    rf.id,
		Term:    rf.Term,
		Reject:  true,
	}
	if m.Term >= rf.Term {
		if m.Term > rf.Term {
			rf.becomeFollower(m.Term, None)
			reply.Term = rf.Term
		}
		if rf.Vote == m.From {
			reply.Reject = false
		} else if rf.Vote == None {
			index, logTerm := rf.RaftLog.LastIndexTerm()
			if m.LogTerm > logTerm || (m.LogTerm == logTerm && m.Index >= index) {
				rf.Vote = m.From
				reply.Reject = false
			}
		}
	}
	rf.msgs = append(rf.msgs, reply)
	DPrintf("reject: %v", reply.Reject)
}

func (rf *Raft) bcastBeat() {
	for peer := range rf.Prs {
		if peer != rf.id {
			rf.sendHeartbeat(peer)
		}
	}
	DPrintf("%d, broadcast Heartbeat", rf.id)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (rf *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	//应当发送commitIndex吗？需要促进Follower的commitIndex更新吗？
	//需要发送prevIndex、prevTerm吗？当然，不然怎么更新commitIndex
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    rf.id,
		Term:    rf.Term,
		//Commit:  rf.RaftLog.committed,
	}
	rf.msgs = append(rf.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (rf *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	reply := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    rf.id,
		Term:    rf.Term,
	}

	if m.Term < rf.Term {
		rf.msgs = append(rf.msgs, reply)
		return
	} else if rf.State != StateFollower || m.Term > rf.Term {
		rf.becomeFollower(m.Term, m.From)
		reply.Term = rf.Term
	}
	rf.Lead = m.From
	rf.electionElapsed = 0
	//TODO 需要在此时更新commitIndex吗？
	//不能单纯的判断commitIndex，lastIndex,m.commit，是有其限制的
	//得带上prevIndex和prevTerm
	rf.msgs = append(rf.msgs, reply)
}

func (rf *Raft) appendPropose(entries []*pb.Entry) {
	startIndex := rf.RaftLog.LastIndex() + 1
	for i, entry := range entries {
		entry.Index = startIndex + uint64(i)
		entry.Term = rf.Term

		rf.RaftLog.entries = append(rf.RaftLog.entries, *entry)
	}
	lastIndex := rf.RaftLog.LastIndex()
	DPrintf("%d, appendPropose %d-%d", rf.id, startIndex, lastIndex)
	rf.Prs[rf.id].Match = lastIndex
	rf.Prs[rf.id].Next = lastIndex + 1
	if len(rf.Prs) == 1 {
		rf.updateCommit()
	} else {
		rf.bcastAppend()
	}
}

func (rf *Raft) bcastAppend() {
	for peer := range rf.Prs {
		if peer != rf.id {
			rf.sendAppend(peer)
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (rf *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var entries []*pb.Entry
	offset, err := rf.RaftLog.getLogLocByIndex(rf.Prs[to].Next)
	if err == ErrCompacted {
		//TODO snapshot
		DPrintf("%d, to %d need snapshot", rf.id, to)
		return false
	} else if err != ErrUnavailable {
		size := len(rf.RaftLog.entries) - offset
		entries = make([]*pb.Entry, size, size)
		for i := 0; i < size; i++ {
			entries[i] = &rf.RaftLog.entries[i+offset]
		}
	}

	prevIndex := rf.Prs[to].Next - 1
	prevTerm, _ := rf.RaftLog.Term(prevIndex)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    rf.id,
		Term:    rf.Term,
		LogTerm: prevTerm,
		Index:   prevIndex,
		Entries: entries,
		Commit:  rf.RaftLog.committed,
	}
	rf.msgs = append(rf.msgs, msg)
	DPrintf("%d, send to %d with log %d-%d", rf.id, to, prevIndex+1, prevIndex+uint64(len(entries)))
	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (rf *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	reply := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    rf.id,
		Term:    rf.Term,
	}
	if m.Term < rf.Term {
		reply.Reject = true
		rf.msgs = append(rf.msgs, reply)
		return
	} else if rf.State != StateFollower || m.Term > rf.Term {
		rf.becomeFollower(m.Term, m.From)
		reply.Term = rf.Term
	}
	rf.Lead = m.From
	rf.electionElapsed = 0

	prevTerm, err := rf.RaftLog.Term(m.Index)
	if prevTerm == m.LogTerm && err == nil {
		reply.Reject = false

		var lastNewEntryIndex uint64

		if len(m.Entries) != 0 {
			//TODO 后发先至的判断是有问题的
			sendLastIndex, sendLastTerm := m.Entries[len(m.Entries)-1].Index, m.Entries[len(m.Entries)-1].Term
			lastNewEntryIndex = sendLastIndex
			myTerm, err := rf.RaftLog.Term(sendLastIndex)
			if err != nil || myTerm != sendLastTerm {
				rf.RaftLog.removeConflict(m.Index)
				for _, entry := range m.Entries {
					rf.RaftLog.entries = append(rf.RaftLog.entries, *entry)
				}
				//TODO 持久化

				DPrintf("append entries %d-%d, commit: %d", m.Index+1, m.Entries[len(m.Entries)-1].Index, m.Commit)
			} else {
				DPrintf("append entries already exist")
			}

		} else {
			lastNewEntryIndex = m.Index
			DPrintf("no entry append, commit: %d", m.Commit)
		}

		if m.Commit > rf.RaftLog.committed {
			rf.RaftLog.committed = min(m.Commit, lastNewEntryIndex)
			//此处不能使用本身的lastIndex，会有逻辑问题，得使用对方发送的最后一个log的Index
		}
		reply.Index = lastNewEntryIndex

		//TODO apply
		//if rf.RaftLog.committed >
	} else {
		end, err := rf.RaftLog.getLogLocByIndex(m.Index)
		if err == ErrUnavailable {
			end = len(rf.RaftLog.entries) - 1
		} else if err == ErrCompacted {
			end = -1
		}
		for ; end >= 0 && rf.RaftLog.entries[end].Term > m.LogTerm; end-- {
		}
		reply.Reject = true

		if end < 0 {
			reply.LogTerm = rf.RaftLog.snapTerm
			reply.Index = rf.RaftLog.snapIndex
			DPrintf("conflictTerm: %d, conflictIndex: %d", reply.LogTerm, reply.Index)

		} else {
			reply.LogTerm = rf.RaftLog.entries[end].Term
			reply.Index = rf.RaftLog.entries[end].Index
			DPrintf("conflictTerm: %d, conflictIndex: %d", reply.LogTerm, reply.Index)
		}
	}
	rf.msgs = append(rf.msgs, reply)
}

func (rf *Raft) countVote() {
	count := 0
	for _, vote := range rf.votes {
		if vote {
			count++
		}
	}
	if count > len(rf.Prs)/2 {
		rf.becomeLeader()
	} else if len(rf.votes)-count > len(rf.Prs)/2 {
		rf.becomeFollower(rf.Term, None)
	}
}

func (rf *Raft) updateCommit() bool {
	majority := len(rf.Prs) / 2
	commit := rf.RaftLog.committed
	for {
		count := 0
		for _, v := range rf.Prs {
			if v.Match > commit {
				count++
				if count > majority {
					commit++
					break
				}
			}
		}
		if count <= majority {
			break
		}
	}
	term, _ := rf.RaftLog.Term(commit)
	if commit > rf.RaftLog.committed && term == rf.Term {
		rf.RaftLog.committed = commit
		//TODO Apply

		return true
	}
	return false
}

func (rf *Raft) getConflictIndex(term, index uint64) uint64 {
	//找到最后一个Term和Index均小于等于(term,index)的 log
	if index < rf.RaftLog.snapIndex || term < rf.RaftLog.snapTerm { //需要snapshot
		return rf.RaftLog.snapIndex
	}

	end, err := rf.RaftLog.getLogLocByIndex(index)
	if err == ErrUnavailable {
		end = len(rf.RaftLog.entries) - 1
	} else if err == ErrCompacted {
		end = -1
	}
	for ; end >= 0 && rf.RaftLog.entries[end].Term > term; end-- {
	}
	if end == len(rf.RaftLog.entries)-1 {
		return rf.RaftLog.entries[end].Index + 1
	}
	return rf.RaftLog.entries[end+1].Index
}

func (rf *Raft) getHardState() pb.HardState {
	return pb.HardState{
		Term:   rf.Term,
		Vote:   rf.Vote,
		Commit: rf.RaftLog.committed,
	}
}

// handleSnapshot handle Snapshot RPC request
func (rf *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (rf *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (rf *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
