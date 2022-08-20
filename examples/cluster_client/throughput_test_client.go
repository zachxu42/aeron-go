package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/corymonroe-coinbase/aeron-go/aeron"
	"github.com/corymonroe-coinbase/aeron-go/aeron/atomic"
	"github.com/corymonroe-coinbase/aeron-go/aeron/idlestrategy"
	"github.com/corymonroe-coinbase/aeron-go/aeron/logbuffer"
	"github.com/corymonroe-coinbase/aeron-go/cluster/client"
)

type TestContext struct {
	ac                    *client.AeronCluster
	messageCount          int
	latencies             []int64
	nextSendKeepAliveTime int64
}

func (ctx *TestContext) OnConnect(ac *client.AeronCluster) {
	fmt.Printf("OnConnect - sessionId=%d leaderMemberId=%d leadershipTermId=%d\n",
		ac.ClusterSessionId(), ac.LeaderMemberId(), ac.LeadershipTermId())
	ctx.ac = ac
	ctx.nextSendKeepAliveTime = time.Now().UnixMilli() + time.Second.Milliseconds()
}

func (ctx *TestContext) OnDisconnect(cluster *client.AeronCluster, details string) {
	fmt.Printf("OnDisconnect - sessionId=%d (%s)\n", cluster.ClusterSessionId(), details)
	ctx.ac = nil
}

func (ctx *TestContext) OnMessage(cluster *client.AeronCluster, timestamp int64,
	buffer *atomic.Buffer, offset int32, length int32, header *logbuffer.Header) {
	recvTime := time.Now().UnixNano()
	msgNo := buffer.GetInt32(offset)
	sendTime := buffer.GetInt64(offset + 12)
	latency := recvTime - sendTime
	//fmt.Println(latency)
	//
	//if latency > time.Second.Nanoseconds() {
	//	fmt.Printf("%s %s %s", time.Now().String(), time.UnixMilli(sendTime/1000000).String(), time.UnixMilli(recvTime/1000000).String())
	//}
	if msgNo < 1 || int(msgNo) > len(ctx.latencies) {
		fmt.Printf("OnMessage - sessionId=%d timestamp=%d pos=%d length=%d latency=%d\n",
			cluster.ClusterSessionId(), timestamp, header.Position(), length, latency)
	} else {
		ctx.latencies[msgNo-1] = latency
		ctx.messageCount++
	}
}

func (ctx *TestContext) OnNewLeader(cluster *client.AeronCluster, leadershipTermId int64, leaderMemberId int32) {
	fmt.Printf("OnNewLeader - sessionId=%d leaderMemberId=%d leadershipTermId=%d\n",
		cluster.ClusterSessionId(), leaderMemberId, leadershipTermId)
}

func (ctx *TestContext) OnError(cluster *client.AeronCluster, details string) {
	fmt.Printf("OnError - sessionId=%d: %s\n", cluster.ClusterSessionId(), details)
}

func (ctx *TestContext) sendKeepAliveIfNecessary() {
	if now := time.Now().UnixMilli(); now > ctx.nextSendKeepAliveTime && ctx.ac != nil && ctx.ac.SendKeepAlive() {
		ctx.nextSendKeepAliveTime += time.Second.Milliseconds()
	}
}

func main() {
	ctx := aeron.NewContext()
	if aeronDir := os.Getenv("AERON_DIR"); aeronDir != "" {
		ctx.AeronDir(aeronDir)
		fmt.Println("aeron dir: ", aeronDir)
	} else if _, err := os.Stat("/dev/shm"); err == nil {
		path := fmt.Sprintf("/dev/shm/aeron-%s", aeron.UserName)
		ctx.AeronDir(path)
		fmt.Println("aeron dir: ", path)
	}

	opts := client.NewOptions()
	opts.IdleStrategy = &idlestrategy.Busy{}
	opts.IngressChannel = "aeron:udp?alias=cluster-client-ingress|endpoint=localhost:20000"
	opts.IngressEndpoints = "0=localhost:20000,1=localhost:21000,2=localhost:22000"
	//opts.EgressChannel = "aeron:udp?alias=cluster-egress|endpoint=localhost:11111"

	listener := &TestContext{}
	clusterClient, err := client.NewAeronCluster(ctx, opts, listener)
	if err != nil {
		panic(err)
	}

	for !clusterClient.IsConnected() {
		opts.IdleStrategy.Idle(clusterClient.Poll())
	}

	messageCount := int32(10000)
	messageSize := int32(256)
	throttleInterval := 10 * time.Microsecond

	sendBuf := atomic.MakeBuffer(make([]byte, messageSize+20))
	padding := atomic.MakeBuffer(make([]byte, messageSize))
	for round := 1; round <= 10; round++ {
		fmt.Printf("starting round #%d\n", round)
		listener.messageCount = 0
		beginTime := time.Now().UnixNano()
		listener.latencies = make([]int64, messageCount)
		latencies := listener.latencies
		for i := range latencies {
			latencies[i] = 0
		}
		ct := len(latencies)
		for i := 1; i <= ct; i++ {
			sendBuf.PutInt32(0, int32(i))
			sendBuf.PutInt32(4, messageCount)
			sendBuf.PutInt32(8, int32(round))
			sendBuf.PutInt64(12, time.Now().UnixNano())
			sendBuf.PutBytes(20, padding, 0, messageSize)
			for {
				if r := clusterClient.Offer(sendBuf, 0, sendBuf.Capacity()); r >= 0 {
					break
				}
				clusterClient.Poll()
				listener.sendKeepAliveIfNecessary()
			}
			now := time.Now()
			for time.Since(now) < throttleInterval {
				clusterClient.Poll()
				listener.sendKeepAliveIfNecessary()
			}
		}
		for listener.messageCount < int(messageCount) {
			pollCt := clusterClient.Poll()
			if pollCt == 0 {
				listener.sendKeepAliveIfNecessary()
			}
			opts.IdleStrategy.Idle(pollCt)
		}
		now := time.Now()
		totalNs := now.UnixNano() - beginTime
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		fmt.Printf("round #%d complete, count=%d min=%d 50%%=%d 90%%=%d 99%%=%d max=%d throughput=%.2f\n",
			round, messageCount, latencies[ct-int(messageCount)]/1000, latencies[ct/2]/1000, latencies[9*ct/10]/1000, latencies[99*(ct/100)]/1000,
			latencies[ct-1]/1000, (float64(messageCount) * 1000000000.0 / float64(totalNs)))

		for time.Since(now) < 2*time.Second {
			listener.sendKeepAliveIfNecessary()
			opts.IdleStrategy.Idle(clusterClient.Poll())
		}
	}
	clusterClient.Close()
	fmt.Println("done")
	time.Sleep(time.Second)
}
