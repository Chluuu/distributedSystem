package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	"math/rand"
	"time"
	"sync"
	"sync/atomic"
	"../labrpc"
	"fmt"
)
// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type ID int
const(
	Follower ID = 0
	Candidate ID =1
	Leader	 ID = 2
)
const(
	ElectionTimeOut = time.Millisecond * 300 // 选举
	HeartBeatTimeout = time.Millisecond * 150 // leader 发送心跳
	RPCTimeout = time.Millisecond * 100 
)
//
// A Go object implementing a single Raft peer.
//
type LogEntry struct{
	Command interface{}
	Term int
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor	int
	log			[]LogEntry

	commitIndex int
	lastApplied int
	nextIndex	[]int
	matchIndex	[]int

	identity	ID


	electionTimer *time.Timer
	appendEntriesTimers []*time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term=rf.currentTerm
	isleader=rf.identity==Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int//Candidate's term
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("ask peer %d to vote\n",rf.me)

	reply.Term=rf.currentTerm
	reply.VoteGranted=false
	if args.Term<rf.currentTerm{
		return
	}else if args.Term==rf.currentTerm{
		if rf.identity==Leader{
			return 
		}
		if rf.votedFor==args.CandidateID{
			reply.VoteGranted=true
			return 
		} 
		if rf.votedFor!=-1&& rf.votedFor!=args.CandidateID{
			return 
		}
	}
	fmt.Printf("peer %d is voting\n",rf.me)

	//if args.Term>rf.currentTerm{
	rf.currentTerm=args.Term
	rf.votedFor=args.CandidateID
	rf.identity=Follower
	reply.VoteGranted=true
	rf.ResetElectionTimer()
	//}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	T:=time.NewTimer(RPCTimeout)
	defer T.Stop()
	
	rpcTimer:=time.NewTimer(RPCTimeout)
	for{
		/*if !rpcTimer.Stop(){
			select{
			case <-rpcTimer.C:
			default:
			}
		}*/
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch:=make(chan bool)
		tmpr:=RequestVoteReply{}
		go func(){
			ok:=rf.peers[server].Call("Raft.RequestVote", args, &tmpr)
			if ok==false{
				time.Sleep(time.Millisecond * 10)
			}
			ch<-ok
		}()
		select{
		case<-T.C:
			return false
		case<-rpcTimer.C:
			continue
		case ok:=<-ch:
			if ok{
				reply.Term=tmpr.Term
				reply.VoteGranted=tmpr.VoteGranted
				/*
				if tmpr.VoteGranted==true {
					ss=1
				}
				fmt.Printf("peer %d  voted %d\n",server,ss)
				*/	
				return ok
			}else{
				continue
			}
		}
		
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	
	log:=LogEntry{
		Command: command,
		Term:	 rf.currentTerm,		
	}
	// Your code here (2B).
	if rf.identity!=Leader{
		isLeader=false
		return index, term, isLeader
	}else{
		rf.mu.Lock()
		rf.log=append(rf.log,log)
		for i,_:=range rf.peers{
			if i==rf.me{
				continue
			}
			rf.matchIndex[index]+=1
		}
		rf.commitIndex+=1
		index=rf.commitIndex
		term=rf.currentTerm 
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) ResetElectionTimer(){
	if !rf.electionTimer.Stop(){
		select{
		case <-rf.electionTimer.C:
		default:
		}
	}
	//rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
}
func (rf *Raft) ResetHeartBeatTimers(){
	for index,_:=range rf.appendEntriesTimers{
		
		if !rf.appendEntriesTimers[index].Stop(){
			select{
			case <-rf.appendEntriesTimers[index].C:
			default:
			}
		}
		//rf.appendEntriesTimers[index].Stop()
		rf.appendEntriesTimers[index].Reset(0)
	}
}
func (rf *Raft) ResetHeartBeatTimer(index int){
	if !rf.appendEntriesTimers[index].Stop(){
		select{
		case <-rf.appendEntriesTimers[index].C:
		default:
		}
	}
	//rf.appendEntriesTimers[index].Stop()
	rf.appendEntriesTimers[index].Reset(HeartBeatTimeout)
}
func (rf *Raft) lastLogTermIndex() (int, int) {
	term := rf.log[len(rf.log)-1].Term
	index := len(rf.log) - 1
	return term, index
}
func(rf *Raft) startElection(){
	rf.mu.Lock()
	fmt.Printf("peer %d start election\n",rf.me)

	if rf.identity==Leader{
		rf.electionTimer.Reset(randElectionTimeout())
		rf.mu.Unlock()
		return
	}
	rf.identity=Candidate
	rf.currentTerm+=1
	rf.votedFor=rf.me
	rf.electionTimer.Reset(randElectionTimeout())

	//lastlogindex,lastlogterm:=rf.lastLogTermIndex()
	args:=RequestVoteArgs{
		Term : rf.currentTerm,
		CandidateID : rf.me,
		LastLogIndex :-1,
		LastLogTerm :-1,
	}
	rf.mu.Unlock()
	
	votesNum:=1
	votesCh:=make(chan bool,len(rf.peers))
	for index,_:=range rf.peers {
		if index==rf.me {
			continue
		}
		go func(ch chan bool,index int){
			reply:=RequestVoteReply{}
			rf.sendRequestVote(index,&args,&reply)
			ch<-reply.VoteGranted
			rf.mu.Lock()
			if reply.Term>rf.currentTerm{
				rf.currentTerm=reply.Term
				rf.identity=Follower
				rf.ResetElectionTimer()
			}
			rf.mu.Unlock()
		}(votesCh,index)
	}
	fmt.Printf("all vote finished\n")
	i:=1
	for {
		isVoted:=<-votesCh
		i+=1
		if isVoted {
			votesNum+=1
		}
		if i == len(rf.peers) || votesNum > len(rf.peers)/2 || i-votesNum > len(rf.peers)/2 {
			break
		}
	}
	fmt.Printf("peer %d get %d votes\n",rf.me,votesNum)

	if votesNum <= len(rf.peers)/2 {
		return
	}
	rf.mu.Lock()
	fmt.Printf("peer %d get %d votes\n",rf.me,votesNum)

	if votesNum>len(rf.peers)/2 && rf.identity==Candidate && rf.currentTerm==args.Term{
		
		rf.identity=Leader
		rf.electionTimer.Reset(randElectionTimeout())
		rf.ResetHeartBeatTimers()
		for index,_:=range rf.peers{
			if index==rf.me{
				continue
			}
			rf.nextIndex[index]=len(rf.log)+1
			rf.matchIndex[index]=0
		}
		rf.mu.Unlock()
		return 
	}
	rf.mu.Unlock()

	
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func randElectionTimeout()time.Duration{
	r:=time.Duration(rand.Int63())%ElectionTimeOut
	return r+ElectionTimeOut
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.identity=Follower
	rf.currentTerm=0
	rf.votedFor=-1

	rf.nextIndex=make([]int,len(rf.peers))
	rf.matchIndex=make([]int,len(rf.peers))

	emptyLog:=LogEntry{}
	rf.log=append(rf.log,emptyLog)
	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex=0
	rf.lastApplied=0
	rf.electionTimer=time.NewTimer(randElectionTimeout())
	rf.appendEntriesTimers=make([]*time.Timer,len(rf.peers))
	for i,_:=range rf.appendEntriesTimers{
		rf.appendEntriesTimers[i]=time.NewTimer(HeartBeatTimeout)
	}
	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())
	fmt.Printf("make peer %d\n",rf.me)
	go func(){
		for {
			select{
			case <- rf.electionTimer.C:
				rf.startElection()
			}
		}
	}()
	apdentryNum:=1
	
	apdentryCh:=make([]chan bool,len(rf.peers))
	for index,_:=range rf.peers{
		if index==rf.me{
			continue
		}
		apdentryCh[index]=make(chan bool)
		fmt.Printf("index1 %d\n",index)
		go func(index int,appdentry []chan bool){
			for {
				select{
				case <-rf.appendEntriesTimers[index].C:
					fmt.Printf("index %d\n",index)
					apdentryCh[index]<-rf.appendEntriestoPeer(index)
				default:
				}
			}
		}(index,apdentryCh)
		}
	go func(){
	for !rf.killed(){
		i:=1
		minCommit:=rf.commitIndex
		for {
			iscommit:=<-apdentryCh[i-1]
			i+=1
			if iscommit {
				apdentryNum+=1
				if minCommit>rf.nextIndex[i-1]-1{
					minCommit=rf.nextIndex[i-1]-1
				}
			}
			if i == len(rf.peers) || apdentryNum > len(rf.peers)/2 || i-apdentryNum > len(rf.peers)/2 {
				break
			}
		}
		if apdentryNum > len(rf.peers)/2{
			rf.mu.Lock()
			for i:=rf.lastApplied+1;i<=minCommit;i+=1{
				msg:=ApplyMsg{
					CommandValid: true,
					Command:	  rf.log[i].Command,
					CommandIndex: i,
				}
				applyCh<-msg
			}
			rf.lastApplied=minCommit
			rf.mu.Unlock()
	}
	}
}()
	return rf
}
