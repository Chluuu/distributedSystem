package raft

import(
	"time"
	"fmt"
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
	if args.PrevLogTerm!=rf.log[args.PrevLogIndex].Term{
		reply.Success=false
		rf.mu.Unlock()
		return
	}
	
	for index,log:=range args.Entries{
		rf.mu.Lock()
		if len(rf.log)> args.PrevLogIndex+1+index {
			//conflict with Entry
			if rf.log[args.PrevLogIndex+1+index].Term!=log.Term{
				rf.log=rf.log[0:args.PrevLogIndex+1+index]
				rf.log=append(rf.log,log)
			}
			//Already exist ,no need to append/overwrite
		}else{
			rf.log=append(rf.log,log)
		}
		rf.mu.Unlock()
	}
	if len(args.Entries)!=0{
		reply.Success=true
	}
	if args.LeaderCommit>rf.commitIndex{
		rf.mu.Lock()
		if args.LeaderCommit<len(rf.log)-1{
			rf.commitIndex=args.LeaderCommit
		}else{
			rf.commitIndex=len(rf.log)-1
		}
		//rf.commitIndex=args.LeaderCommit<args.PrevLogIndex+len(args.Entries)?args.LeaderCommit:args.PrevLogIndex+len(args.Entries)
		rf.mu.Unlock()
	}
	rf.currentTerm=args.Term
	rf.identity=Follower
	rf.ResetElectionTimer()
	rf.mu.Unlock()
}
func(rf *Raft)appendEntriestoPeer(server int)bool{
	T:=time.NewTimer(RPCTimeout)
	defer T.Stop()

	entrs:=[]LogEntry{}
	success:=false
	args:=AppendEntryArgs{
		Term:	  rf.currentTerm,
		LeaderID: rf.me,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:	entrs,
		LeaderCommit:	rf.commitIndex,
	}
	if rf.identity==Leader{
		for i:=rf.nextIndex[server];i<=rf.matchIndex[server];i+=1{
		entrs=append(entrs,rf.log[i])
		}
		args.PrevLogIndex=rf.nextIndex[server]-1
		fmt.Printf("%d\n",rf.nextIndex[server]-1)
		args.PrevLogTerm=rf.log[rf.nextIndex[server]-1].Term
	}
	reply:=AppendEntryReply{}

	
	for !rf.killed(){
		rf.mu.Lock()
		if rf.identity!=Leader{
			fmt.Printf("pass2\n")
			rf.ResetHeartBeatTimer(server)
			rf.mu.Unlock()
			return success
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
			ok:=rf.peers[server].Call("Raft.AppendEntry", &args, &tmpr)
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
				success=reply.Success
				break
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
		return success
	}
	rf.nextIndex[server]=rf.matchIndex[server]+1
	if rf.identity != Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return success
	}


	rf.mu.Unlock()
	return success
}