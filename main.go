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
	"runtime"
	"strconv"
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
	runtime.GOMAXPROCS(runtime.NumCPU())
	isServer := flag.Bool("s", false, "run as a server")
	localAddr := flag.String("laddr", ":12345", "local address host:port to listen on")
	remoteAddr := flag.String("ad", "localhost:12345", "remote server address host:port")
	numConn := flag.Int("c", 1, "number of connections")
	numConcurrentConn := flag.Int("cc", 1, "max number of concurrent connections")
	numRequestPerConn := flag.Int("r", 100, "number of requests per connection")
	requestIntervalStr := flag.String("rp", "10ms", "period for sending request,give 0 for full speed")
	connectIntervalStr := flag.String("ci", "0ms", "min interval duration between connections")

	requestSize := flag.Int("rqs", 128, "Request size in byte. Larger than "+strconv.Itoa(TIME_STAMP_LENGTH+1)+" .")
	responseSize := flag.Int("rps", 256, "Response size in byte. Setting for server. Larger than "+strconv.Itoa(TIME_STAMP_LENGTH+1)+" .")

	tcpNoDelay := flag.Bool("tcpNoDelay", false, "set tcpNoDelay")

	connStatFileName := flag.String("fc", "ConnStat.txt", "file name to save status of connections")
	latencyFileName := flag.String("fr", "Latencys.txt", "file name to save latencys of requests")

	outputHistogram := flag.Bool("shisto", false, "Output statistics of histogram")
	rttStepStr := flag.String("sd", "10us", "rtt statistics step duration in histogram")
	numRttSteps := flag.Int("sn", 100, "number of rtt statistics steps in histogram")
	outputQuiet := flag.Bool("q", false, "just show valid RTT number min avg max std RTT")
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
		server := NewServer("tcp", *localAddr, *responseSize, *tcpNoDelay)
		fmt.Println("True response size: ", server.GetResponseSize(), " bytes.")
		server.StartListen()
	} else {
		//is client
		opt := NewTestOption(
			"tcp", *remoteAddr,
			*numConn, *numConcurrentConn, connectInterval, *numRequestPerConn,
			requestInterval, *requestSize,
			*tcpNoDelay,
			rttStep, *numRttSteps,
		)
		if !*outputQuiet {
			fmt.Println("True request size: ", opt.GetRequestSize(), " bytes.")
		}
		tester := NewTester(opt)
		respSize, err := tester.GetResponseSize()
		if err != nil {
			log.Fatal("Can't get response: ", err)
		} else {
			if !*outputQuiet {
				fmt.Println("Response size from server: ", respSize, " bytes.")
			}
		}
		tester.DoTest()
		if *outputQuiet {
			fmt.Println(
				tester.Stat.NumRtt, "\t",
				tester.Stat.MinRtt, "\t",
				tester.Stat.AvgRtt, "\t",
				tester.Stat.MaxRtt, "\t",
				tester.Stat.StdRtt,
			)
		} else if tester.Stat.NumRtt != 0 {
			fmt.Println("Number of connects stop with error: ", tester.Stat.ErrConnCount)
			fmt.Println("Number of valid RTT: ", tester.Stat.NumRtt)
			fmt.Println("min/avg/max/std of RTT:", tester.Stat.MinRtt, "/", tester.Stat.AvgRtt, "/", tester.Stat.MaxRtt, "/", tester.Stat.StdRtt)
			if *outputHistogram {
				fmt.Println("RTT Histogram( step = ", rttStep, "): ")
				for k, v := range tester.Stat.RttHisto {
					fmt.Println(k, "\t", v)
				}
			}
			tester.SaveConnStat(*connStatFileName)
			tester.SaveLatencys(*latencyFileName)
		} else {
			fmt.Println("No response available!")
		}
	}
}

const TIME_STAMP_LENGTH = 15

type Msg struct {
	TimeStamp []byte
	Data      []byte
}

type Server struct {
	NetType      string
	LocalAddr    string
	TcpNoDelay   bool
	ResponseData []byte
}

func NewServer(netType string, localAddr string, responseSize int, tcpNoDelay bool) *Server {
	return &Server{
		NetType:      netType,
		LocalAddr:    localAddr,
		TcpNoDelay:   tcpNoDelay,
		ResponseData: Krand(responseSize-1-TIME_STAMP_LENGTH, KC_RAND_KIND_ALL),
	}
}

func (s *Server) GetResponseSize() int {
	return TIME_STAMP_LENGTH + len(s.ResponseData) + 1
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
		if s.TcpNoDelay {
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				if err := tcpConn.SetNoDelay(true); err != nil {
					log.Fatal(err)
				}
			}
		}
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			log.Fatal(err)
		}
		go s.handleTestConnection(conn)
	}
}

func (s *Server) handleTestConnection(c net.Conn) {
	sendChan := make(chan *Msg, 256)

	go func(c net.Conn, sendChan chan *Msg) { //sending
		bw := bufio.NewWriter(c)
		for {
			m := <-sendChan
			err := sendMsg(bw, m)
			if err != nil {
				c.Close()
				return
			}
		}
	}(c, sendChan)

	br := bufio.NewReader(c) //receiving and handling
	for {
		//recv request
		m, err := waitMsg(br)
		if err != nil {
			c.Close()
			return
		}

		//handle msg
		m.Data = s.ResponseData

		//send response
		sendChan <- &m
	}
}

type TestResultStat struct {
	ErrConnCount int
	NumRtt       int
	MaxRtt       time.Duration
	MinRtt       time.Duration
	AvgRtt       time.Duration
	StdRtt       time.Duration
	RttHisto     []int
}

type TestOption struct {
	NetType           string
	RemoteAddr        string
	NumConn           int
	NumConcurrentConn int
	ConnectInterval   time.Duration
	NumRequestPerConn int
	RequestInterval   time.Duration
	RequestData       []byte
	TcpNoDelay        bool

	RttStep     time.Duration
	NumRttSteps int
}

func NewTestOption(netType string,
	remoteAddr string,
	numConn int,
	numConcurrentConn int,
	connectInterval time.Duration,
	numRequestPerConn int,
	requestInterval time.Duration,
	requestSize int,
	tcpNoDelay bool,
	rttStep time.Duration,
	numRttSteps int,
) *TestOption {
	return &TestOption{
		NetType:           netType,
		RemoteAddr:        remoteAddr,
		NumConn:           numConn,
		NumConcurrentConn: numConcurrentConn,
		ConnectInterval:   connectInterval,
		NumRequestPerConn: numRequestPerConn,
		RequestInterval:   requestInterval,
		RequestData:       Krand(requestSize-TIME_STAMP_LENGTH-1, KC_RAND_KIND_ALL),
		TcpNoDelay:        tcpNoDelay,
		RttStep:           rttStep,
		NumRttSteps:       numRttSteps,
	}
}

func (o *TestOption) GetRequestSize() int {
	return len(o.RequestData) + TIME_STAMP_LENGTH + 1
}

type Tester struct {
	//options
	Opt *TestOption

	//list of testConn
	testConnList []*testConn

	//used by testConn
	startChan  chan bool      //channel for controling concurrent
	finishChan chan *testConn //channel for collecting results

	//result
	Stat TestResultStat
}

func NewTester(o *TestOption) *Tester {
	return &Tester{Opt: o}
}
func (t *Tester) DoTest() {
	t.startChan = make(chan bool, t.Opt.NumConcurrentConn)
	t.finishChan = make(chan *testConn, t.Opt.NumConcurrentConn*2)

	//launch test connections
	t.testConnList = make([]*testConn, t.Opt.NumConn)
	go func() {
		for i := 0; i < t.Opt.NumConn; i++ {
			tc := &testConn{t: t}
			t.testConnList[i] = tc
			t.startChan <- true
			go tc.doTest()
			time.Sleep(t.Opt.ConnectInterval)
		}
	}()

	errConnCount := 0
	for i := 0; i < t.Opt.NumConn; i++ {
		tc := <-t.finishChan
		if tc.err != nil {
			errConnCount += 1
		}
	}
	t.Stat.ErrConnCount = errConnCount
	t.doStatistics()

}
func (t *Tester) GetResponseSize() (size int, err error) {
	conn, err := net.Dial(t.Opt.NetType, t.Opt.RemoteAddr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	timeStamp, err := time.Now().MarshalBinary()
	if err != nil {
		return 0, err
	}
	req := Msg{timeStamp, t.Opt.RequestData}
	resp, err := requestMsg(bw, br, &req)
	return len(resp.TimeStamp) + len(resp.Data), err
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

func (t *Tester) doStatistics() {
	numRtt := 0
	minRtt := time.Duration(time.Hour)

	for _, tc := range t.testConnList {
		l := len(tc.latencyList)
		if l > 0 {
			minRtt = tc.latencyList[0]
		}
		numRtt += l
	}
	t.Stat.NumRtt = numRtt
	if numRtt == 0 {
		return
	}

	//calculate mean value min/max value and histgram
	sumRtt := float64(0)
	maxRtt := time.Duration(0)
	rttHisto := make([]int, t.Opt.NumRttSteps)
	for _, tc := range t.testConnList {
		for _, rtt := range tc.latencyList {
			stepNum := int64(rtt / t.Opt.RttStep)
			if stepNum >= int64(t.Opt.NumRttSteps) {
				stepNum = int64(t.Opt.NumRttSteps - 1)
			}
			rttHisto[stepNum]++
			sumRtt += float64(rtt)
			if rtt > maxRtt {
				maxRtt = rtt
			}
			if rtt < minRtt {
				minRtt = rtt
			}
		}
	}
	avgRtt := time.Duration(sumRtt / float64(numRtt))

	//calculate standard deviation
	tmp := float64(0)
	for _, tc := range t.testConnList {
		for _, rtt := range tc.latencyList {
			d := float64(rtt - avgRtt)
			tmp += d * d
		}
	}
	t.Stat.AvgRtt = avgRtt
	t.Stat.MaxRtt = maxRtt
	t.Stat.MinRtt = minRtt
	t.Stat.RttHisto = rttHisto
	t.Stat.StdRtt = time.Duration(math.Sqrt(tmp / float64(numRtt)))
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
	var ticker *time.Ticker
	//dial connection
	conn, err := net.Dial(tc.t.Opt.NetType, tc.t.Opt.RemoteAddr)

	defer func() {
		if conn != nil {
			conn.Close()
		}
		if ticker != nil {
			ticker.Stop()
		}
		<-tc.t.startChan
		tc.t.finishChan <- tc
		tc.stopTime = time.Now()
	}()
	if tc.t.Opt.TcpNoDelay {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetNoDelay(true); err != nil {
				tc.err = err
				return
			}
		}
	}

	if err != nil {
		tc.err = err
		return
	}
	bw := bufio.NewWriter(conn)
	br := bufio.NewReader(conn)

	tc.latencyList = make([]time.Duration, 0, tc.t.Opt.NumRequestPerConn)
	recvErrorChan := make(chan error, 1)

	//receive and handle response
	go func() {
		for i := 0; i < tc.t.Opt.NumRequestPerConn; i++ {
			m, err := waitMsg(br)
			if err != nil {
				recvErrorChan <- err
				return
			}
			var sendTime time.Time
			err = sendTime.UnmarshalBinary(m.TimeStamp)
			if err != nil { //normarlly won't happen
				recvErrorChan <- err
				return
			}
			tc.latencyList = append(tc.latencyList, time.Now().Sub(sendTime))
		}
		recvErrorChan <- nil
	}()

	//send request
	needTick := tc.t.Opt.RequestInterval != 0
	if needTick {
		ticker = time.NewTicker(tc.t.Opt.RequestInterval)
	}
	for i := 0; i < tc.t.Opt.NumRequestPerConn; i++ {
		timeStamp, err := time.Now().MarshalBinary()
		if err != nil {
			tc.err = err
			return
		}
		err = sendMsg(bw, &Msg{timeStamp, tc.t.Opt.RequestData})
		if err != nil {
			tc.err = err
			return
		}
		if needTick {
			<-ticker.C
		}
	}
	tc.err = <-recvErrorChan
	return
}

func requestMsg(bw *bufio.Writer, br *bufio.Reader, request *Msg) (response Msg, err error) {
	err = sendMsg(bw, request)
	if err != nil {
		return response, err
	}
	return waitMsg(br)
}

func sendMsg(bw *bufio.Writer, m *Msg) error {
	_, err := bw.Write(m.TimeStamp)
	if err != nil {
		return err
	}
	_, err = bw.Write(m.Data)
	if err != nil {
		return err
	}
	err = bw.WriteByte('\n')
	if err != nil {
		return err
	}
	return bw.Flush()
}

func waitMsg(br *bufio.Reader) (m Msg, err error) {
	m.TimeStamp = make([]byte, TIME_STAMP_LENGTH)

	for i := 0; i < TIME_STAMP_LENGTH; i++ {
		m.TimeStamp[i], err = br.ReadByte()
		if err != nil {
			return m, err
		}
	}
	m.Data, err = br.ReadBytes('\n')
	return m, err
}
