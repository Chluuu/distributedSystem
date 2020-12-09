package raft

import(
	"time"
)
type AppendEntryArgs struct {
	// Your data here (2A, 2B).
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	Entries	[]LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	// Your data here (2A).
	Term int
	Success bool
}

func(rf *Raft)AppendEntry(args *AppendEntryArgs,reply *AppendEntryReply){
	rf.mu.Lock()
	reply.Term=rf.currentTerm
	reply.Success=false
	if args.Term<rf.currentTerm{
		reply.Success=false
		rf.mu.Unlock()
		return 
	}
	rf.currentTerm=args.Term
	rf.identity=Follower
	rf.ResetElectionTimer()
	rf.mu.Unlock()
}
func(rf *Raft)appendEntriestoPeer(server int){
	T:=time.NewTimer(RPCTimeout)
	defer T.Stop()

	args:=AppendEntryArgs{
		Term:	  rf.currentTerm,
		LeaderID: rf.me,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:	[]LogEntry{},
		LeaderCommit:	rf.commitIndex,
	}
	reply:=AppendEntryReply{}
	
	for !rf.killed(){
		rf.mu.Lock()
		if rf.identity!=Leader{
			rf.ResetHeartBeatTimer(server)
			rf.mu.Unlock()
			return
		}
		rf.ResetHeartBeatTimer(server)
		rf.mu.Unlock()

		if !T.Stop(){
			select{
			case <-T.C:
			default:
			}
		}
		T.Reset(RPCTimeout)

		tmpr:=AppendEntryReply{}
		ch:=make(chan bool)
		go func(args *AppendEntryArgs,tmpr *AppendEntryReply){
			ok:=rf.peers[server].Call("Raft.AppendEntry", args, tmpr)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}(&args,&tmpr)
		select{
		case<-T.C:
			continue
		case ok:=<-ch:
			if ok{
				reply.Term=tmpr.Term
				reply.Success=tmpr.Success
			}else{
				continue
			}
		}

		
	}
	rf.mu.Lock()
	if reply.Term >rf.currentTerm{
		rf.currentTerm=reply.Term
		rf.identity=Follower
		rf.ResetElectionTimer()
		rf.mu.Unlock()
		return 
	}
	if rf.identity != Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()
	return
}