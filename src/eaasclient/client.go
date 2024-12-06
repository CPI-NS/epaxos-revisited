package main

import (
	"bufio"
  "os"
//	"github.com/efficient/epaxos/src/dlog"
	"flag"
	"fmt"
	"github.com/PlatformLab/epaxos-revisited/src/genericsmrproto"
	"log"
	"github.com/PlatformLab/epaxos-revisited/src/masterproto"
//	"math/rand/v2"
	"net"
	"net/rpc"
	"runtime"
	"github.com/PlatformLab/epaxos-revisited/src/state"
  "github.com/EaaS"
	"time"
  "strconv"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. Defaults to false.")
var rounds *int = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var eps *int = flag.Int("eps", 0, "Send eps more messages per round than the client will wait for (to discount stragglers). Defaults to 0.")
var conflicts *int = flag.Int("c", -1, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")

/* EAAS */
var stackedMultiPaxos *bool = flag.Bool("smp", false, "Stacked Multi Paxos, Expects an environment variable saying which leader. Defaults to false.")
/* END EAAS */

var N int

var successful []int

var rarray []int
var rsp []bool

/* EAAS moved things */
var id int32 = 0
var leader int

var writers []*bufio.Writer
var readers []*bufio.Reader
var servers []net.Conn

var master *rpc.Client
/* EAAS moved things end */

func main() {

  EaaS.EaasInit()
  EaaS.EaasRegister(batchPut, "batch_put")
  EaaS.EaasRegister(Put, "put")
  EaaS.EaasRegister(Get, "get")
  EaaS.EaasRegister(Set, "set")
  EaaS.EaasRegister(dbInit, "db_init")
  EaaS.EaasRegister(beginTx, "begin_tx")
  EaaS.EaasRegister(commitTx, "commit_tx")
  EaaS.EaasRegister(rollbackTx, "rollback_tx")
  
  StartEpaxos()

  EaaS.EaasStartGRPC()

 // keys := make([]int64, 2)
 // keys[0] = 0
 // keys[1] = 1
 // values := make([]int32, 2)
 // values[0] = 5
 // values[1] = 6

 // fmt.Println("Calling put")
 // batchPut(keys,nil, values, 2, 2)

//  for true {}
}

func StartEpaxos() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

//	randObj := rand.New(rand.NewSource(42))
//	zipf := rand.NewZipf(randObj, *s, *v, uint64(*reqsNb / *rounds + *eps))

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

  var err error
	master, err = rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if err != nil {
		log.Fatalf("Error connecting to master\n")
	}

	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("Error making the GetReplicaList RPC")
	}

	N = len(rlReply.ReplicaList)
	//servers := make([]net.Conn, N)
	servers = make([]net.Conn, N)
  //readers := make([]*bufio.Reader, N)
  readers = make([]*bufio.Reader, N)
	//writers := make([]*bufio.Writer, N)
	writers = make([]*bufio.Writer, N)

	//rarray = make([]int, *reqsNb / *rounds + *eps)
//	karray := make([]int64, *reqsNb / *rounds + *eps)
//	put := make([]bool, *reqsNb / *rounds + *eps)
//	perReplicaCount := make([]int, N)
//	test := make([]int, *reqsNb / *rounds + *eps)
//	for i := 0; i < len(rarray); i++ {
//		r := rand.Intn(N)
//		rarray[i] = r
//		if i < *reqsNb / *rounds {
//			perReplicaCount[r]++
//		}
//
//		if *conflicts >= 0 {
//			r = rand.Intn(100)
//			if r < *conflicts {
//				karray[i] = 42
//			} else {
//				karray[i] = int64(43 + i)
//			}
//			r = rand.Intn(100)
//			if r < *writes {
//				put[i] = true
//			} else {
//				put[i] = false
//			}
//		} else {
//			karray[i] = int64(zipf.Uint64())
//			test[karray[i]]++
//		}
//	}
//	if *conflicts >= 0 {
//		fmt.Println("Uniform distribution")
//	} else {
//		fmt.Println("Zipfian distribution:")
//		//fmt.Println(test[0:100])
//	}

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d\n", i)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])
	}

	successful = make([]int, N)
	leader = 0
  //leader = rand.IntN(N)

	if *noLeader == false {
		reply := new(masterproto.GetLeaderReply)
		if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
			log.Fatalf("Error making the GetLeader RPC\n")
		}
		leader = reply.LeaderId
		log.Printf("The leader is replica %d\n", leader)
	}

	//var id int32 = 0
  //done := make(chan bool, N)
	//args := genericsmrproto.Propose{id, state.Command{state.PUT, 0, 0}, 0}

//	before_total := time.Now()

//	for j := 0; j < *rounds; j++ {
//
//		n := *reqsNb / *rounds
//
//		if *check {
//			rsp = make([]bool, n)
//			for j := 0; j < n; j++ {
//				rsp[j] = false
//			}
//		}
//
//		if *noLeader {
//      fmt.Println("calling wait replies epaxos")
//			for i := 0; i < N; i++ {
//				go waitReplies(readers, i, perReplicaCount[i], done)
//			}
//		} else {
//      fmt.Println("calling wait replies not-epaxos")
//			go waitReplies(readers, leader, n, done)
//		}

//		before := time.Now()
//
//		for i := 0; i < n+*eps; i++ {
//      batchPut();
//		}
//		for i := 0; i < N; i++ {
//			writers[i].Flush()
//		}
//
//		err := false
//		if *noLeader {
//			for i := 0; i < N; i++ {
//				e := <-done
//				err = e || err
//			}
//		} else {
//			err = <-done
//		}
//
//		after := time.Now()
//
//		fmt.Printf("Round took %v\n", after.Sub(before))
//
//		if *check {
//			for j := 0; j < n; j++ {
//				if !rsp[j] {
//					fmt.Println("Didn't receive", j)
//				}
//			}
//		}
//
//		if err {
//			if *noLeader {
//				N = N - 1
//			} else {
//				reply := new(masterproto.GetLeaderReply)
//				master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
//				leader = reply.LeaderId
//				log.Printf("New leader is replica %d\n", leader)
//			}
//		}
//	}
//
//	after_total := time.Now()
//	fmt.Printf("Test took %v\n", after_total.Sub(before_total))
//
//	s := 0
//	for _, succ := range successful {
//		s += succ
//	}
//
//	fmt.Printf("Successful: %d\n", s)
//
//	for _, client := range servers {
//		if client != nil {
//			client.Close()
//		}
//	}
//	master.Close()
}

func Get(key int64, _ []int32, _ int, _ []int32) int {
  return EaaS.EAAS_W_EC_SUCCESS
}

func Put(key int64, _ []int32, values []int32, _ int) int {
  keys := make([]int64, 1)
  keys[0] = key
  batchPut(keys, nil, values, 0, 1)
  return EaaS.EAAS_W_EC_SUCCESS
}

func Set(key int64, values []int32) int {
//  keys := make([]int64, 1)
//  keys[0] = key
//  batchPut(keys, nil, values, 0, 1)
  return EaaS.EAAS_W_EC_SUCCESS
}

func batchPut(keys []int64, _ []int32, values[]int32, _ int, batch_size int) int { 
  fmt.Println("BatchPut Start")
	args := genericsmrproto.Propose{id, state.Command{state.PUT, 0, 0}, 0}
  args.CommandId = id
  //if put[i] {
    args.Command.Op = state.PUT
  //} else {
  //  args.Command.Op = state.GET
  //}

  for i, _ := range keys {
    args.Command.K = state.Key(keys[i])
    args.Command.V = state.Value(values[0])
    args.Timestamp = time.Now().UnixNano()
    /* No fast pass optimization */
    if !*fast {
      if *noLeader {
       // leader = rarray[i]

        /* Get leader for this batch */
        /* 
           so the controlle thinks there is 6 servers becuase
           of the master but there is only 5
         */
          sutAddr := os.Getenv("SUT_ADDR")
          leader, _ = strconv.Atoi(sutAddr[len(sutAddr)-1:]) 
          leader = leader - 2
       //   fmt.Println("sut_addr: ", leader)
       //   leader = leader % N
       //leader = rand.IntN(N)

      } else if *stackedMultiPaxos {
        leaderEnvVar := os.Getenv("LEADER")
        //fmt.Println("Leader Env Var: ", leaderEnvVar)
        leader , _ = strconv.Atoi(leaderEnvVar)
      }
     
//      fmt.Println("Sending batch to leader: ", leader)
      writers[leader].WriteByte(genericsmrproto.PROPOSE)
      args.Marshal(writers[leader])
    } else {
      //send to everyone
      for rep := 0; rep < N; rep++ {
        writers[rep].WriteByte(genericsmrproto.PROPOSE)
        args.Marshal(writers[rep])
        writers[rep].Flush()
      }
    }
  }
  //}
  //fmt.Println("Sent", id)
  //id++
  //if id%100 == 0 {
  //  for i := 0; i < N; i++ {
  //    writers[i].Flush()
  //  }
  //}

  writers[leader].Flush()
  done := make(chan bool, N)
  if *noLeader {
 //   fmt.Println("calling wait replies epaxos")
//    for i := 0; i < N; i++ {
      go waitReplies(readers, leader, batch_size, done)
 //   }
  } else {
//    fmt.Println("calling wait replies not-epaxos")
    //fmt.Println("Sending for response from leader: ", leader)
    go waitReplies(readers, leader, batch_size, done)
  }

  err := false
 // fmt.Println("Waiting on reply from replicas")
  if *noLeader {
    //for i := 0; i < N; i++ {
      e := <-done
      err = e || err
    //}
  } else {
    err = <-done
  }

  fmt.Println("BatchPUt end")
  if err {
    if *noLeader {
      N = N - 1
    } else {
      reply := new(masterproto.GetLeaderReply)
      master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
      leader = reply.LeaderId
      log.Printf("New leader is replica %d\n", leader)
    }
    return EaaS.EAAS_W_EC_INTERNAL_ERROR
  } else {
    return EaaS.EAAS_W_EC_SUCCESS
  }
}

func dbInit(_ int) int {
  return EaaS.EAAS_W_EC_SUCCESS
}

func beginTx() int {
  return EaaS.EAAS_W_EC_SUCCESS
}

func commitTx() int {
  return EaaS.EAAS_W_EC_SUCCESS
}

func rollbackTx() int {
  return EaaS.EAAS_W_EC_SUCCESS
}


func waitReplies(readers []*bufio.Reader, leader int, n int, done chan bool) {
  //fmt.Println("In Wait replies")
	e := false

  //reply := new(genericsmrproto.ProposeReplyTS)
  reply := new(genericsmrproto.ProposeReply)
  //fmt.Println("right before for loop")
  for i := 0; i < n; i++ {
 //   fmt.Println("Waiting for replies...")
		if err := reply.Unmarshal(readers[leader]); err != nil {
			fmt.Println("Error when reading:", err)
			e = true
			continue
		} //else {
 //     fmt.Println("Got reply")
 //   }
  //  fmt.Println(reply.OK)
  //  fmt.Println(reply.CommandId)
	//	fmt.Println(reply.Value)
	//	fmt.Println(reply.Timestamp)
		if *check {
			if rsp[reply.CommandId] {
				fmt.Println("Duplicate reply", reply.CommandId)
			}
			rsp[reply.CommandId] = true
		}
		if reply.OK != 0 {
			successful[leader]++
		}
	}
	done <- e
}
