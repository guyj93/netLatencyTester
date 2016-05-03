// netLatencyTester project main.go
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
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
	if size < 0 {
		size = 0
	}
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
	numThread := flag.Int("cpus", 0, "number of cpus will be used, 0 for number of logic cores on your machine")
	isServer := flag.Bool("s", false, "run as a server")
	localAddr := flag.String("laddr", ":12345", "local address host:port to listen on")
	remoteAddr := flag.String("ad", "localhost:12345", "remote server address host:port")
	numConn := flag.Int("c", 1, "number of connections")
	numConcurrentConn := flag.Int("cc", 1, "max number of concurrent connections")
	numRequestPerConn := flag.Int("r", 100, "number of requests per connection")
	requestIntervalStr := flag.String("rp", "10ms", "period for sending request,give 0 for full speed")
	connectIntervalStr := flag.String("ci", "0ms", "min interval duration between connections")

	requestSize := flag.Int("rqs", 128, "Request size in byte. Larger than "+strconv.Itoa(MSG_EXTRA_SIZE)+" .")
	responseSize := flag.Int("rps", 256, "Response size in byte. Setting for server. Larger than "+strconv.Itoa(MSG_EXTRA_SIZE)+" .")

	waitResponse := flag.Bool("wr", false, "wait response before sending another request. Won't wait by default.")

	bufioSize := flag.Int("bufio", 4096, "buffer size of bufio in Byte, 0 for no bufio, -1 for auto size.")
	tcpNoDelay := flag.Bool("tcpNoDelay", false, "set tcpNoDelay")

	connStatFileName := flag.String("fc", "", "file name to save status of connections, empty for not to save the report")
	latencyFileName := flag.String("fr", "", "file name to save latencys of requests, empty for not to save the report")

	outputHistogram := flag.Bool("shisto", false, "show statistics of histogram")
	rttStepStr := flag.String("sd", "10us", "rtt statistics step duration in histogram")
	numRttSteps := flag.Int("sn", 100, "number of rtt statistics steps in histogram")
	outputQuiet := flag.Bool("q", false, "quiet mode, just show number, min, avg, max, std (of RTT), realRequestRate, realTxSpeed, realRxSpeed")
	flag.Parse()
	if *numThread == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		if n := runtime.GOMAXPROCS(*numThread); n < 1 {
			log.Fatal("Set number of cpus failed.")
		}
	}
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
		server := NewServer("tcp", *localAddr, *responseSize, *tcpNoDelay, *bufioSize)
		fmt.Println("True response size: ", server.GetResponseSize(), " bytes.")
		server.StartListen()
	} else {
		//is client
		opt := NewClientOption(
			"tcp", *remoteAddr,
			*numConn, *numConcurrentConn, connectInterval, *numRequestPerConn,
			requestInterval, *requestSize,
			*waitResponse,
			*tcpNoDelay,
			*bufioSize,
			rttStep, *numRttSteps,
		)

		if !*outputQuiet {
			fmt.Println("True request size: ", opt.GetRequestSize(), " bytes.")
		}
		tester := NewClient(opt)
		respSize, err := tester.GetResponseSize()
		if err != nil {
			log.Fatal("Can't get response: ", err)
		} else {
			if !*outputQuiet {
				fmt.Println("Response size from server: ", respSize, " bytes.")
			}
		}
		tester.DoTest()
		testTimeSeconds := float64(tester.StopTime.Sub(tester.StartTime)) / float64(time.Second)
		requestRate := float64(0)
		tSpeed := float64(0)
		rSpeed := float64(0)
		if testTimeSeconds != float64(0) {
			requestRate = float64(tester.Stat.NumRtt) / (testTimeSeconds)
			tSpeed = requestRate * float64(tester.Opt.GetRequestSize()) * 8
			rSpeed = requestRate * float64(respSize) * 8
		}
		if *outputQuiet {
			fmt.Println(
				tester.Stat.NumRtt, "\t",
				tester.Stat.MinRtt, "\t",
				tester.Stat.AvgRtt, "\t",
				tester.Stat.MaxRtt, "\t",
				tester.Stat.StdRtt, "\t",
				requestRate, "\t",
				tSpeed, "\t",
				rSpeed,
			)
		} else {
			if tester.Stat.NumRtt != 0 {
				fmt.Println("Number of connects stop with error: ", tester.Stat.ErrConnCount)
				fmt.Println("Number of valid RTT: ", tester.Stat.NumRtt)
				fmt.Println("min/avg/max/std of RTT:", tester.Stat.MinRtt, "/", tester.Stat.AvgRtt, "/", tester.Stat.MaxRtt, "/", tester.Stat.StdRtt)
				fmt.Println("Request per second: ", requestRate)
				fmt.Println("Average transfer speed: ", tSpeed, "bps")
				fmt.Println("Average receive speed: ", rSpeed, "bps")
				if *outputHistogram {
					fmt.Println("RTT Histogram( step = ", rttStep, "): ")
					for k, v := range tester.Stat.RttHisto {
						fmt.Println(k, "\t", v)
					}
				}
			} else {
				fmt.Println("No response available!")
			}
		}
		if *connStatFileName != "" {
			tester.SaveConnStat(*connStatFileName)
		}
		if *latencyFileName != "" {
			tester.SaveLatencys(*latencyFileName)
		}
	}
}

const TIME_STAMP_LENGTH = 8
const DATA_SIZE_DATA_LENGTH = 4
const MSG_EXTRA_SIZE = TIME_STAMP_LENGTH + DATA_SIZE_DATA_LENGTH

type Msg struct {
	timeStampData []byte
	dataSizeData  []byte
	data          []byte
}

func NewMsg() *Msg {
	return &Msg{
		make([]byte, TIME_STAMP_LENGTH),
		make([]byte, DATA_SIZE_DATA_LENGTH),
		nil,
	}
}

func (m *Msg) SetTimeStamp(timeStamp int64) {
	binary.LittleEndian.PutUint64(m.timeStampData, uint64(timeStamp))
	return
}

func (m *Msg) GetTimeStamp() (timeStamp int64) {
	return int64(binary.LittleEndian.Uint64(m.timeStampData))
}

//notice that the function won't copy data
func (m *Msg) SetDataAs(src *Msg) {
	m.dataSizeData = src.dataSizeData
	m.data = src.data
}

func (m *Msg) SetData(data []byte) {
	binary.LittleEndian.PutUint32(m.dataSizeData, uint32(len(data)))
	m.data = data
}

func (m *Msg) GetData() []byte {
	return m.data
}

func (m *Msg) Send(w io.Writer) (err error) {
	if _, err = w.Write(m.timeStampData); err != nil {
		return err
	}
	if _, err = w.Write(m.dataSizeData); err != nil {
		return err
	}
	_, err = w.Write(m.data)
	if bw, ok := w.(*bufio.Writer); ok {
		err = bw.Flush()
	}
	return err
}

func (m *Msg) Wait(r io.Reader) (err error) {
	//read timeStampData
	if _, err = io.ReadFull(r, m.timeStampData); err != nil {
		return err
	}

	if _, err = io.ReadFull(r, m.dataSizeData); err != nil {
		return err
	}

	dataSize := binary.LittleEndian.Uint32(m.dataSizeData)
	m.data = make([]byte, dataSize)
	_, err = io.ReadFull(r, m.data)
	return err
}

type Server struct {
	NetType          string
	LocalAddr        string
	TcpNoDelay       bool
	BufioSize        int
	ResponseData     []byte
	responseTemplate *Msg
}

func NewServer(netType string, localAddr string, responseSize int, tcpNoDelay bool, bufioSize int) *Server {
	return &Server{
		NetType:      netType,
		LocalAddr:    localAddr,
		TcpNoDelay:   tcpNoDelay,
		BufioSize:    bufioSize,
		ResponseData: Krand(responseSize-MSG_EXTRA_SIZE, KC_RAND_KIND_ALL),
	}
}

func (s *Server) GetResponseSize() int {
	return len(s.ResponseData) + MSG_EXTRA_SIZE
}

func (s *Server) StartListen() {
	l, err := net.Listen(s.NetType, s.LocalAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	fmt.Println("Listen requests on ", s.LocalAddr)

	s.responseTemplate = NewMsg()
	s.responseTemplate.SetData(s.ResponseData)

	for {
		conn, err := l.Accept()
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			log.Fatal(err)
		}
		if s.TcpNoDelay {
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				if err := tcpConn.SetNoDelay(true); err != nil {
					conn.Close()
					log.Fatal(err)
				}
			}
		}
		go s.handleTestConnection(conn)
	}
}

func (s *Server) handleTestConnection(conn net.Conn) {
	var w io.Writer
	var r io.Reader
	if s.BufioSize == 0 {
		w = conn
		r = conn
	} else if s.BufioSize < 0 {
		w = bufio.NewWriterSize(conn, len(s.responseTemplate.data)+MSG_EXTRA_SIZE)
		r = bufio.NewReader(conn) //default size 4096
	} else {
		w = bufio.NewWriterSize(conn, s.BufioSize)
		r = bufio.NewReaderSize(conn, s.BufioSize)
	}
	sendChan := make(chan *Msg, 256)
	go func() { //sending
		for {
			m := <-sendChan
			err := m.Send(w)
			if err != nil {
				conn.Close()
				return
			}
		}
	}()

	for {
		//recv request
		m := NewMsg()
		err := m.Wait(r)
		if err != nil {
			conn.Close()
			return
		}

		//handle msg
		m.SetDataAs(s.responseTemplate)

		//send response
		sendChan <- m
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

type ClientOption struct {
	//Test options
	NetType           string
	RemoteAddr        string
	NumConn           int
	NumConcurrentConn int
	ConnectInterval   time.Duration
	NumRequestPerConn int
	RequestInterval   time.Duration
	RequestData       []byte
	WaitResponse      bool
	TcpNoDelay        bool
	BufioSize         int

	//Statistics options
	RttStep     time.Duration
	NumRttSteps int
}

func NewClientOption(netType string,
	remoteAddr string,
	numConn int,
	numConcurrentConn int,
	connectInterval time.Duration,
	numRequestPerConn int,
	requestInterval time.Duration,
	requestSize int,
	waitResponse bool,
	tcpNoDelay bool,
	bufioSize int,
	rttStep time.Duration,
	numRttSteps int,
) *ClientOption {
	return &ClientOption{
		NetType:           netType,
		RemoteAddr:        remoteAddr,
		NumConn:           numConn,
		NumConcurrentConn: numConcurrentConn,
		ConnectInterval:   connectInterval,
		NumRequestPerConn: numRequestPerConn,
		RequestInterval:   requestInterval,
		RequestData:       Krand(requestSize-MSG_EXTRA_SIZE, KC_RAND_KIND_ALL),
		TcpNoDelay:        tcpNoDelay,
		BufioSize:         bufioSize,
		WaitResponse:      waitResponse,
		RttStep:           rttStep,
		NumRttSteps:       numRttSteps,
	}
}

func (o *ClientOption) GetRequestSize() int {
	return len(o.RequestData) + MSG_EXTRA_SIZE
}

type Client struct {
	//options
	Opt *ClientOption

	//a guess of responseSize
	responseSizeGuess int

	//list of testConn
	testConnList []*testConn

	//used by testConn
	startChan  chan bool      //channel for controling concurrent
	finishChan chan *testConn //channel for collecting results

	//result
	StartTime time.Time
	StopTime  time.Time
	Stat      TestResultStat
}

func NewClient(o *ClientOption) *Client {
	return &Client{Opt: o}
}
func (c *Client) DoTest() {
	c.startChan = make(chan bool, c.Opt.NumConcurrentConn)
	c.finishChan = make(chan *testConn, c.Opt.NumConcurrentConn*2)
	c.testConnList = make([]*testConn, c.Opt.NumConn)

	c.StartTime = time.Now()
	//launch test connections
	go func() {
		for i := 0; i < c.Opt.NumConn; i++ {
			tc := &testConn{c: c}
			c.testConnList[i] = tc
			c.startChan <- true
			go tc.doTest()
			time.Sleep(c.Opt.ConnectInterval)
		}
	}()

	errConnCount := 0
	for i := 0; i < c.Opt.NumConn; i++ {
		tc := <-c.finishChan
		if tc.err != nil {
			errConnCount += 1
		}
	}
	c.Stat.ErrConnCount = errConnCount

	c.StopTime = time.Now()

	c.doStatistics()
}

//can be used to check if we can access the server, and help guess the responseSize to auto determine bufio size
func (c *Client) GetResponseSize() (size int, err error) {
	conn, err := net.Dial(c.Opt.NetType, c.Opt.RemoteAddr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	m := NewMsg()
	m.SetTimeStamp(time.Now().UnixNano())
	m.SetData(c.Opt.RequestData)
	if err = m.Send(bw); err != nil {
		return 0, err
	}
	err = m.Wait(br)
	return len(m.GetData()) + MSG_EXTRA_SIZE, err
}

func (c *Client) SaveConnStat(fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	fmt.Fprintln(f, c.Opt.NumConn)
	fmt.Fprintln(f, "ConnID\tNumResp\tStartTime\tStopTime\tError")
	for k, tc := range c.testConnList {
		fmt.Fprintln(f, k, "\t", len(tc.latencyList), "\t", tc.startTime, "\t", tc.stopTime, "\t", tc.err)
	}
	return f.Close()
}

func (c *Client) SaveLatencys(fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	fmt.Fprintln(f, "ConnID\tRqstID\tLatency")
	for i, tc := range c.testConnList {
		for j, latency := range tc.latencyList {
			fmt.Fprintln(f, i, "\t", j, "\t", latency)
		}
	}
	return f.Close()
}

func (c *Client) doStatistics() {
	numRtt := 0
	minRtt := time.Duration(time.Hour)

	for _, tc := range c.testConnList {
		l := len(tc.latencyList)
		if l > 0 {
			minRtt = tc.latencyList[0]
		}
		numRtt += l
	}
	c.Stat.NumRtt = numRtt
	if numRtt == 0 {
		return
	}

	//calculate mean value min/max value and histgram
	sumRtt := float64(0)
	maxRtt := time.Duration(0)
	rttHisto := make([]int, c.Opt.NumRttSteps)
	for _, tc := range c.testConnList {
		for _, rtt := range tc.latencyList {
			stepNum := int64(rtt / c.Opt.RttStep)
			if stepNum >= int64(c.Opt.NumRttSteps) {
				stepNum = int64(c.Opt.NumRttSteps - 1)
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
	for _, tc := range c.testConnList {
		for _, rtt := range tc.latencyList {
			d := float64(rtt - avgRtt)
			tmp += d * d
		}
	}
	c.Stat.AvgRtt = avgRtt
	c.Stat.MaxRtt = maxRtt
	c.Stat.MinRtt = minRtt
	c.Stat.RttHisto = rttHisto
	c.Stat.StdRtt = time.Duration(math.Sqrt(tmp / float64(numRtt)))
}

type testConn struct {
	c           *Client
	startTime   time.Time
	stopTime    time.Time
	err         error
	latencyList []time.Duration
}

func (tc *testConn) doTest() {
	tc.startTime = time.Now()
	var ticker *time.Ticker
	//dial connection
	conn, err := net.Dial(tc.c.Opt.NetType, tc.c.Opt.RemoteAddr)

	defer func() {
		if conn != nil {
			conn.Close()
		}
		if ticker != nil {
			ticker.Stop()
		}
		<-tc.c.startChan
		tc.c.finishChan <- tc
		tc.stopTime = time.Now()
	}()

	if err != nil {
		tc.err = err
		return
	}

	if tc.c.Opt.TcpNoDelay {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetNoDelay(true); err != nil {
				tc.err = err
				return
			}
		}
	}
	var w io.Writer
	var r io.Reader
	if tc.c.Opt.BufioSize == 0 {
		w = conn
		r = conn
	} else if tc.c.Opt.BufioSize < 0 {
		w = bufio.NewWriterSize(conn, len(tc.c.Opt.RequestData)+MSG_EXTRA_SIZE)
		r = bufio.NewReader(conn)
	} else {
		w = bufio.NewWriterSize(conn, tc.c.Opt.BufioSize)
		r = bufio.NewReaderSize(conn, tc.c.Opt.BufioSize)
	}

	tc.latencyList = make([]time.Duration, 0, tc.c.Opt.NumRequestPerConn)
	request := NewMsg()
	request.SetTimeStamp(time.Now().UnixNano())
	request.SetData(tc.c.Opt.RequestData)
	response := NewMsg()
	if tc.c.Opt.WaitResponse { //will wait response before sending another message
		//send request, wait and handle response
		needTick := tc.c.Opt.RequestInterval != 0
		if needTick {
			ticker = time.NewTicker(tc.c.Opt.RequestInterval)
		}
		for i := 0; i < tc.c.Opt.NumRequestPerConn; i++ {
			sendTime := time.Now().UnixNano()
			if err = request.Send(w); err != nil {
				tc.err = err
				return
			}
			if err = response.Wait(r); err != nil {
				tc.err = err
				return
			}
			tc.latencyList = append(tc.latencyList, time.Duration(time.Now().UnixNano()-sendTime))
			if needTick {
				<-ticker.C
			}
		}

	} else { //won't wait response before sending another message

		//launch routine for receiving and handling response
		recvErrorChan := make(chan error, 1)
		go func() {
			var sendTime int64
			var err error
			for i := 0; i < tc.c.Opt.NumRequestPerConn; i++ {
				if err = response.Wait(r); err != nil {
					recvErrorChan <- err
					return
				}
				sendTime = response.GetTimeStamp()
				tc.latencyList = append(tc.latencyList, time.Duration(time.Now().UnixNano()-sendTime))
			}
			recvErrorChan <- nil
		}()

		//send request
		needTick := tc.c.Opt.RequestInterval != 0
		if needTick {
			ticker = time.NewTicker(tc.c.Opt.RequestInterval)
		}
		for i := 0; i < tc.c.Opt.NumRequestPerConn; i++ {
			request.SetTimeStamp(time.Now().UnixNano())
			if err = request.Send(w); err != nil {
				tc.err = err
				return
			}
			if needTick {
				<-ticker.C
			}
		}
		tc.err = <-recvErrorChan //record error happen in receive routine
	}
	return
}
