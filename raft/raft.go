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
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
//	"github.com/stretchr/testify/assert"
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

	// ElectionTick is the number of Node.Tick invocations(调用) that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// leader和follower无联系，leader过期时间
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
//Match指可以follower目前匹配到的log id,正常情况下next = match + 1
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

	//log appendrespone,记录followers的反应
	//appendresponeNum map[uint64]uint64

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
	electionTimeout int
	randElectionTimeout int
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
	raft := &Raft{}
	raft.id = c.ID
	rand.Seed(time.Now().Unix())
	raft.electionTimeout = c.ElectionTick
	raft.setRandElectionTimeout()
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.Prs = make(map[uint64]*Progress)
	raft.votes = make(map[uint64]bool)
	raft.State = StateFollower
	raft.Term = 0 //初始化任期为0
	
	raft.RaftLog = newLog(c.Storage)
	//raft.State = StateFollower
	raft.Prs = make(map[uint64]*Progress)
	for _,id := range c.peers{
		raft.Prs[id] = &Progress{}
	}

	return raft
}
func (r *Raft)setRandElectionTimeout(){
	r.randElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.id == to {
		return false
	}
	index := r.Prs[to].Next
	arrIndex := r.RaftLog.getArrIndexByIndex(uint64(index))
	term,err :=  r.RaftLog.Term(index - 1)
	if err != nil{
		panic(err)
	}	
	targetEng := make([]*pb.Entry, 0)
	if arrIndex != -1{
		e := r.RaftLog.entries[arrIndex:]
		for index,_ := range e{
			targetEng = append(targetEng, &e[index])
		}
	}
	m := pb.Message{
		From: r.id,
		To: to,
		Term: r.Term,
		Commit: r.RaftLog.committed,
		MsgType: pb.MessageType_MsgAppend,
		Entries: targetEng,
		LogTerm: term,
		Index: index - 1,
	}
	r.msgs = append(r.msgs, m)	
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	//msgs []pb.Message
	if to == r.id{
		return
	}
	m := pb.Message{
		From: r.id,
		To: to,
		Term: r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) sendHeartbeatAll() {
	for id,_ := range r.Prs{
		r.sendHeartbeat(id)
	}
}
	

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	
	switch r.State {
	case StateFollower:	
		r.electionElapsed += 1
		if r.electionElapsed >= r.randElectionTimeout{
			r.electionElapsed = 0
			r.becomeCandidate()
			r.newEpochVote()
		}
	case StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed >= r.randElectionTimeout{
			r.electionElapsed = 0
			r.becomeCandidate()
			r.newEpochVote()
		}
		
	case StateLeader:
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed == r.heartbeatTimeout{
			r.heartbeatElapsed = 0
			r.sendHeartbeatAll()
		}
	}
	
	
}

// becomeFollower transform this peer's state to Follower
// raft divides time into terms of arbitrary length
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
//	r.Vote = None
	r.setRandElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term += 1
	r.State = StateCandidate
	for i,_ := range r.votes{
		r.votes[i] = false
	}
	r.votes[r.id] = true
	//r.Vote = 0
	r.id_vote(r.id)
	r.setRandElectionTimeout()
	
}
func (r *Raft) newEpochVote(){
	lastTerm, lastIndex := r.RaftLog.LastInfo()
	for id,_ := range r.Prs{
		//msgs []pb.Message
		if id == r.id{
			continue
		}
		m := pb.Message{
			From: r.id,
			To: id,
			Term: r.Term,
			MsgType: pb.MessageType_MsgRequestVote,
			LogTerm: lastTerm,
			Index: lastIndex,
		}
		r.msgs = append(r.msgs, m)

	}
}
func (r *Raft) id_vote(id uint64){
	r.votes[id] = true
	agreeNum := 0
	for _,isVote := range r.votes{
		if isVote{
			agreeNum++
		}
	}
	if uint64(agreeNum) > uint64(len(r.Prs)/2){
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateCandidate {
		r.State = StateLeader
		//r.Term += 1
		
		lastIndex := r.RaftLog.LastIndex()
		entry := pb.Entry{Data: nil,
			Term: r.Term,
			Index: lastIndex + 1,
		}
		
		r.RaftLog.entries = append(r.RaftLog.entries, entry)
		
		for id,_ := range r.Prs{
			r.Prs[id].Next = lastIndex + 1
			r.Prs[id].Match = 0
		}
		r.Prs[r.id].Match = lastIndex + 1
		r.Prs[r.id].Next = lastIndex + 2
		//数据同步
		for id,_ := range r.Prs{
			r.sendAppend(id)
		}
	}


}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//r.Term = m.Term
	//mycode
	if r.Term <= m.Term && m.MsgType == pb.MessageType_MsgAppend  {
		if r.Term != m.Term{
			r.becomeFollower(m.Term,m.From)
		}
		r.handleAppendEntries(m)
	}
	if r.Term <= m.Term && m.MsgType == pb.MessageType_MsgHeartbeat {
		if r.Term != m.Term{
			r.becomeFollower(m.Term,m.From)
		}
		r.handleHeartbeat(m)
	}

	if m.MsgType == pb.MessageType_MsgRequestVote{
		r.handleVote(m)
	}	
	
	//mycode finish
	switch r.State {
	case StateFollower:
		if m.MsgType == pb.MessageType_MsgHup{
			r.becomeCandidate()
			r.newEpochVote()
		}
		
			
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse && r.Term == m.Term{
			r.votes[m.From] = !m.Reject
			if !m.Reject{
				r.id_vote(m.From)
			}
		}
		if r.Term <= m.Term && m.MsgType == pb.MessageType_MsgAppend {
			r.becomeFollower(m.Term,m.From)
			r.handleAppendEntries(m)
		}
		if m.MsgType == pb.MessageType_MsgHup{
			r.becomeCandidate()
			r.newEpochVote()
		}
		
	case StateLeader:
		if m.MsgType == pb.MessageType_MsgBeat{
			r.sendHeartbeatAll()
		}
		if m.MsgType == pb.MessageType_MsgPropose{
			lastIndex := r.RaftLog.LastIndex()
			next := uint64(0)
			for _,item := range m.Entries{
				next+=1
				entry := pb.Entry{
					Index: lastIndex + next,
					Term: r.Term,
					Data: item.Data,
				}
				r.RaftLog.entries = append(r.RaftLog.entries,entry)
				r.Prs[r.id].Match = lastIndex + next
				r.Prs[r.id].Next = lastIndex + next + 1
			}
			if len(r.Prs) == 1 {
				r.RaftLog.committed = lastIndex + next
			}
			for to,_ := range r.Prs{
				r.sendAppend(to)
			}
		}
		if m.MsgType == pb.MessageType_MsgAppendResponse{
			if m.Reject == false{
				r.Prs[m.From].Match =  m.Index
				r.Prs[m.From].Next = m.Index + 1
				term, err := r.RaftLog.Term(m.Index)
				if err != nil{
					panic(err)
				}
				if r.Term == term && r.RaftLog.committed < m.Index{
					num := 0 //self
					for _,pr := range r.Prs{
						if pr.Match >= m.Index{
							num++
						}
					}
					if num > len(r.Prs)/2 {
						r.RaftLog.committed = m.Index
						// 广播提交信息
						for id,_ := range r.Prs{
							r.sendAppend(id)
						}
					}
					//fmt.Println(r.RaftLog.committed)
				}
			}else{
				r.Prs[m.From].Next -= 1
				r.Prs[m.From].Next = min(r.Prs[m.From].Next,m.Index + 1)
				r.sendAppend(m.From)
			}
		}
	}
	return nil
}
// my code
// func (r *Raft) sendAppendWithEntrys(entries []*pb.Entry){
// 	index := r.RaftLog.LastIndex() - uint64(len(entries))
// 	term,err := r.RaftLog.Term(index)
// 	if err != nil{
// 		panic(err)
// 	}
// 	for to,_ := range r.Prs{
// 		if to != r.id{	
// 			m := pb.Message{
// 				From: r.id,
// 				To: to,
// 				Term: r.Term,
// 				Commit: r.RaftLog.committed,
// 				MsgType: pb.MessageType_MsgAppend,
// 				Entries: entries,
// 				LogTerm: term,
// 				Index: index,
// 			}
// 			r.msgs = append(r.msgs, m)
// 		}
// 	}
// }
// code finish
func (r *Raft) handleVote(m pb.Message){
	isReject := true
	lastTerm, lastIndex := r.RaftLog.LastInfo()
	if  r.Term < m.Term || (r.Term == m.Term && (r.Vote == m.From || r.Vote == None)) {
			if (lastTerm < m.LogTerm)||(lastTerm == m.LogTerm && lastIndex <= m.Index){
			r.becomeFollower(m.Term,m.From)
			r.Vote = m.From
			isReject = false
		}
	}
	

	voteM := pb.Message{
		From: r.id,
		To: m.From,
		Term: r.Term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject: isReject,
	}
	r.msgs = append(r.msgs, voteM)
}
// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	term,err := r.RaftLog.Term(m.Index)
	if  term == m.LogTerm{
		arrIndex := r.RaftLog.getArrIndex(m.Index, term) + 1
		arrIndex2 := 0
		for ;arrIndex2 < len(m.Entries) && int(arrIndex) + arrIndex2 < len(r.RaftLog.entries);arrIndex2++{
			e1 := r.RaftLog.entries[arrIndex + arrIndex2]
			e2 := m.Entries[arrIndex2]
			if e1.Term != e2.Term || e1.Index != e2.Index {
				if e1.Index <= r.RaftLog.stabled{
					if arrIndex + arrIndex2 - 1 < 0{
						r.RaftLog.stabled = 0
					}else {
						r.RaftLog.stabled = r.RaftLog.entries[arrIndex + arrIndex2 - 1].Index
					}
				}
				break
			}
		}
		
		if arrIndex2  < len(m.Entries){
			r.RaftLog.entries = r.RaftLog.entries[:arrIndex + arrIndex2]
			e := m.Entries[arrIndex2:]
			for _,entry := range e{
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
		}
		
		r.RaftLog.committed = m.Commit
		//lastTerm, lastIndex := r.RaftLog.LastInfo()
		appendrespone := pb.Message{
			From: r.id,
			To: m.From,
			Term: r.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index: r.RaftLog.LastIndex(),
			Reject: false,
		}
		r.msgs = append(r.msgs, appendrespone)
		return

	}
	if err != nil || term != m.LogTerm{
		//panic(errors.New("not found entry with the index"))
		appendrespone := pb.Message{
			From: r.id,
			To: m.From,
			Term: r.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index: r.RaftLog.LastIndex(),
			Reject: true,
		}
		r.msgs = append(r.msgs, appendrespone)
		return
	}
	

	//mycode finish

	/**
	switch r.State {
	case StateFollower:	

	case StateCandidate:
		//r.becomeFollower(m.Term,)
	case StateLeader:
	}
	**/
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	heartbeatResponse := pb.Message{
		From: r.id,
		To: m.From,
		Term: r.Term,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		
	}
	r.msgs = append(r.msgs, heartbeatResponse)
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
