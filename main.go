// netLatencyTester project main.go
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"time"
)

const (
	KC_RAND_KIND_NUM   = 0
	KC_RAND_KIND_LOWER = 1
	KC_RAND_KIND_UPPER = 2
	KC_RAND_KIND_ALL   = 3
)

func Krand(size int, kind int) []byte {
	kinds := [][]int{[]int{10, 48}, []int{26, 97}, []int{26, 65}}
	result := make([]byte, size)
	is_all := kind > 2 || kind < 0
	rand.Seed(time.Now().UnixNano())
	var ikind int
	for i := 0; i < size; i++ {
		if is_all { // random ikind
			ikind = rand.Intn(3)
		}
		scope, base := kinds[ikind][0], kinds[ikind][1]
		result[i] = uint8(base + rand.Intn(scope))
	}
	return result
}

func main() {

	isServer := flag.Bool("s", false, "run as a server")
	localAddr := flag.String("laddr", ":12345", "local address host:port to listen on")
	remoteAddr := flag.String("ad", "localhost:12345", "remote server address host:port")
	numConn := flag.Int("c", 1, "number of connections")
	numConcurrentConn := flag.Int("cc", 1, "max number of concurrent connections")
	numRequestPerConn := flag.Int("r", 100, "number of requests per connection")
	requestIntervalStr := flag.String("i", "10ms", "interval duration between requests")
	connectIntervalStr := flag.String("ci", "0ms", "min interval duration between connections")
	rttStepStr := flag.String("sd", "10us", "rtt statistics step duration")
	numRttSteps := flag.Int("sn", 100, "number of rtt statistics steps")

	requestSize := flag.Int("rqs", 128, "Request size in byte")
	responseSize := flag.Int("rps", 256, "Response size in byte, setting for server")

	connStatFileName := flag.String("fs", "ConnStat.txt", "file name to save status of connections")
	latencyFileName := flag.String("fl", "Latencys.txt", "file name to save latencys of requests")

	flag.Parse()

	rttStep, err := time.ParseDuration(*rttStepStr)
	if err != nil {
		log.Fatal(err)
	}

	requestInterval, err := time.ParseDuration(*requestIntervalStr)
	if err != nil {
		log.Fatal(err)
	}
	connectInterval, err := time.ParseDuration(*connectIntervalStr)
	if err != nil {
		log.Fatal(err)
	}
	if *isServer {
		server := Server{
			NetType:      "tcp",
			LocalAddr:    *localAddr,
			ResponseData: Krand(*responseSize-1, KC_RAND_KIND_ALL),
		}
		server.StartListen()
	} else {
		opt := TestOption{
			NetType:           "tcp",
			RemoteAddr:        *remoteAddr,
			NumConn:           *numConn,
			NumConcurrentConn: *numConcurrentConn,
			ConnectInterval:   connectInterval,
			NumRequestPerConn: *numRequestPerConn,
			RttStep:           rttStep,
			NumRttSteps:       *numRttSteps,
			RequestInterval:   requestInterval,
			RequestData:       Krand(*requestSize-1, KC_RAND_KIND_ALL),
		}
		tester := Tester{Opt: opt}
		respSize, err := tester.GetResponseSize()
		if err != nil {
			log.Fatal("Can't get response: ", err)
		} else {
			fmt.Println("Response size from server: ", respSize)
		}
		tester.DoTest()

		tester.Statistics()
		if tester.Stat.NumRtt != 0 {
			fmt.Println("Number of RTT: ", tester.Stat.NumRtt)
			fmt.Println("Avergage value of RTT: ", tester.Stat.AvgRtt)
			fmt.Println("Standard Deviation of RTT: ", tester.Stat.StdRtt)
			fmt.Println("RTT Histogram(step = ", rttStep, "): ")
			for k, v := range tester.Stat.RttHisto {
				fmt.Println(k, "\t", v)
			}
			tester.SaveConnStat(*connStatFileName)
			tester.SaveLatencys(*latencyFileName)
		} else {
			fmt.Println("No response available!")
		}
	}
}

type Server struct {
	NetType      string
	LocalAddr    string
	ResponseData []byte
}

func (s *Server) StartListen() {
	l, err := net.Listen(s.NetType, s.LocalAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	fmt.Println("Listen requests on ", s.LocalAddr)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go s.handleTestConnection(conn)
	}
}
func (s *Server) handleTestConnection(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)
	for {
		_, err := waitData(br)
		if err != nil {
			return
		}
		err = sendData(bw, s.ResponseData)
		if err != nil {
			return
		}
	}
}

type TestResultStat struct {
	NumRtt   int
	AvgRtt   time.Duration
	StdRtt   time.Duration
	RttHisto []int
}

type TestOption struct {
	NetType           string
	RemoteAddr        string
	NumConn           int
	NumConcurrentConn int
	ConnectInterval   time.Duration
	NumRequestPerConn int
	RttStep           time.Duration
	NumRttSteps       int
	RequestInterval   time.Duration
	RequestData       []byte
}

type Tester struct {
	//options
	Opt TestOption

	//list of testConn
	testConnList []*testConn

	//used by testConn
	startChan  chan bool      //channel for controling concurrent
	finishChan chan *testConn //channel for collecting results

	//result
	Stat         TestResultStat
	numRtt       int
	sumRtt       time.Duration
	errConnCount int
}

func (t *Tester) DoTest() {
	t.startChan = make(chan bool, t.Opt.NumConcurrentConn)
	t.finishChan = make(chan *testConn, t.Opt.NumConcurrentConn*2)

	//launch test connections
	t.testConnList = make([]*testConn, t.Opt.NumConn)
	go func() {
		timer := time.NewTimer(0)
		for i := 0; i < t.Opt.NumConn; i++ {
			<-timer.C
			timer = time.NewTimer(t.Opt.ConnectInterval)
			tc := &testConn{t: t}
			t.testConnList[i] = tc
			t.startChan <- true
			go tc.doTest()
		}
	}()

	t.errConnCount = 0
	for i := 0; i < t.Opt.NumConn; i++ {
		tc := <-t.finishChan
		if tc.err != nil {
			t.errConnCount += 1
		}
	}

}
func (t *Tester) GetResponseSize() (size int, err error) {
	conn, err := net.Dial(t.Opt.NetType, t.Opt.RemoteAddr)
	if err != nil {
		return 0, err
	}
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	resp, err := request(bw, br, t.Opt.RequestData)
	return len(resp), err
}

func (t *Tester) SaveConnStat(fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	fmt.Fprintln(f, t.Opt.NumConn)
	fmt.Fprintln(f, "ConnID\tNumResp\tStartTime\tStopTime\tError")
	for k, tc := range t.testConnList {
		fmt.Fprintln(f, k, "\t", len(tc.latencyList), "\t", tc.startTime, "\t", tc.stopTime, "\t", tc.err)
	}
	return f.Close()
}

func (t *Tester) SaveLatencys(fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	fmt.Fprintln(f, "ConnID\tRqstID\tLatency")
	for i, tc := range t.testConnList {
		for j, latency := range tc.latencyList {
			fmt.Fprintln(f, i, "\t", j, "\t", latency)
		}
	}
	return f.Close()
}

func (t *Tester) Statistics() {
	numRtt := 0
	for _, tc := range t.testConnList {
		numRtt += len(tc.latencyList)
	}
	t.Stat.NumRtt = numRtt
	if numRtt == 0 {
		return
	}

	//calculate mean value and histgram
	sumRtt := time.Duration(0)
	rttHisto := make([]int, t.Opt.NumRttSteps)
	for _, tc := range t.testConnList {
		for _, rtt := range tc.latencyList {
			stepNum := int64(rtt / t.Opt.RttStep)
			if stepNum >= int64(t.Opt.NumRttSteps) {
				stepNum = int64(t.Opt.NumRttSteps - 1)
			}
			rttHisto[stepNum]++
			sumRtt += rtt
		}
	}
	avgRtt := sumRtt / time.Duration(numRtt)
	t.Stat.AvgRtt = avgRtt
	t.Stat.RttHisto = rttHisto

	//calculate standard deviation
	stdRtt := time.Duration(0)
	for _, tc := range t.testConnList {
		for _, rtt := range tc.latencyList {
			d := rtt - avgRtt
			stdRtt += d * d
		}
	}
	stdRtt = time.Duration(math.Sqrt(float64(stdRtt) / float64(numRtt)))
	t.Stat.StdRtt = stdRtt
}

type testConn struct {
	t           *Tester
	startTime   time.Time
	stopTime    time.Time
	err         error
	latencyList []time.Duration
}

func (tc *testConn) doTest() {
	tc.startTime = time.Now()
	conn, err := net.Dial(tc.t.Opt.NetType, tc.t.Opt.RemoteAddr)
	if err != nil {
		tc.err = err
		<-tc.t.startChan
		tc.t.finishChan <- tc
		tc.stopTime = time.Now()
		return
	}
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	tc.latencyList = make([]time.Duration, 0, tc.t.Opt.NumRequestPerConn)
	for i := 0; i < tc.t.Opt.NumRequestPerConn; i++ {
		sendTime := time.Now()
		_, err = request(bw, br, tc.t.Opt.RequestData)
		if err != nil {
			conn.Close()
			tc.err = err
			<-tc.t.startChan
			tc.t.finishChan <- tc
			tc.stopTime = time.Now()
			return
		}

		rtt := time.Now().Sub(sendTime)
		stepNum := int64(rtt / tc.t.Opt.RttStep)
		if stepNum >= int64(tc.t.Opt.NumRttSteps) {
			stepNum = int64(tc.t.Opt.NumRttSteps - 1)
		}
		tc.latencyList = append(tc.latencyList, rtt)
		time.Sleep(tc.t.Opt.RequestInterval)
	}
	conn.Close()
	tc.err = nil
	<-tc.t.startChan
	tc.t.finishChan <- tc
	tc.stopTime = time.Now()
	return
}

func request(bw *bufio.Writer, br *bufio.Reader, requestData []byte) (responseData []byte, err error) {
	err = sendData(bw, requestData)
	if err != nil {
		return nil, err
	}
	return waitData(br)
}

func sendData(bw *bufio.Writer, data []byte) error {
	_, err := bw.Write(data)
	if err != nil {
		return err
	}
	err = bw.WriteByte('\n')
	if err != nil {
		return err
	}
	return bw.Flush()
}

func waitData(br *bufio.Reader) (data []byte, err error) {
	return br.ReadBytes('\n')
}
