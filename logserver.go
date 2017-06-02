package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/donnie4w/go-logger/logger"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/go-nsq"
)

// 创建通道
var datachannel chan []byte = make(chan []byte, 100000)
var nsqchannel chan []byte = make(chan []byte, 100000)

// 收到的包的总数
var TotalRecvCount int64

var gStatus uint32 = 0

//var signalChan chan bool = make(chan bool, 1)

type GJson struct {
	Master bool // 主机
	Slave  bool // 从机
}

var (
	flagSet = flag.NewFlagSet("G Transfer", flag.ExitOnError)
	config  = flagSet.String("config", "", "path to config file")

	listenIP   = flagSet.String("listen-ip", "127.0.0.1", "listen ip")
	listenPort = flagSet.Int("listen-port", 8000, "listen port")

	// g server config
	server_addr = flagSet.String("server-addr", "127.0.0.1:5000", "server listening address")
	// magic_code must be 4 bytes
	magic_code = flagSet.String("magic-code", "", "g magic code")
	// magic ip is a intergar of 4 bytes
	magic_ip = flagSet.Int("magic-ip", 0, "g magic ip")
	// time-intervals to fetch g status
	fetchinterval = flagSet.Int("fetchinterval", 5, "time-intervals to fetch g status")
	// http url of g status
	httpurl = flagSet.String("httpurl", "", "http url of g status")

	console     = flagSet.Bool("console", true, "print log output console")
	level       = flagSet.Int("level", 0, "print log by set level. 0:default,1:debug,2:info,3:warn,4:error,5:fatal")
	logdir      = flagSet.String("logdir", "", "save log file in logdir")
	logfilename = flagSet.String("logfilename", "", "print log write to logfilename")
	lognum      = flagSet.Int("lognum", 5, "number of logs")
	logfilesize = flagSet.Int64("logfilesize", 10, "size of logfile")

	nsqserver = flagSet.String("nsq-server", "127.0.0.1:4150", "nsqd server ip:port")
	sendTopic = flagSet.String("send-topic", "test_topic", "nsq publish message topic")

	nsqservers = StringArray{}
)

type Options struct {
	Nsqserver  string   `flag:"nsq-server"`                    // NSQ消息队列ip地址
	Nsqservers []string `flag:"nsq-servers" cfg:"nsq_servers"` // NSQ消息队列ip地址
	SendTopic  string   `flag:"send-topic"`                    // NSQ topic

	ListenIP   string `flag:"listen-ip"`   // udp服务监听ip
	ListenPort int    `flag:"listen-port"` // udp服务监听端口

	ServerAddr   string `flag:"server-addr"` // server listening address
	MagicCode    string `flag:"magic-code"`
	MagicIP      uint32 `flag:"magic-ip"`
	TimeInterval int    `flag:"fetchinterval"`
	HttpUrl      string `flag:"httpurl"`

	Console      bool   `flag:"console"`     // 是否打印到控制台
	Level        int    `flag:"level"`       // 日志等级：ALL、INFO、DEBUG、WARN、ERROR、FATAL、OFF
	LogDir       string `flag:"logdir"`      // 存放文件日志目录
	LogFileName  string `flag:"logfilename"` // 保存日志的文件名
	LogFileLevel string `flag:"level"`       // 日志级别
	LogNum       int32  `flag:"lognum"`      // 日志文件数
	LogFileSize  int64  `flag:"logfilesize"` // 每个日志文件大小
}

type StringArray []string

func (a *StringArray) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *StringArray) String() string {
	return strings.Join(*a, ",")
}

func init() {
	flagSet.Var(&nsqservers, "nsq-servers", "lookupd HTTP address (may be given multiple times)")
}

func Stated() {
	var nodesend int64

	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			totalCount := atomic.LoadInt64(&TotalRecvCount)
			sendOneCount := totalCount - nodesend
			logger.Info("Per Second Static: TotalRecvCounts:(" + strconv.FormatInt(totalCount, 10) + ")\t 1Sec RecvCounts:(" + strconv.FormatInt(sendOneCount, 10) + ")")
			nodesend = totalCount
		}
	}
}

func main() {
	// rand seed
	rand.Seed(time.Now().UTC().UnixNano())

	//读取配置
	flagSet.Parse(os.Args[1:])
	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}
	opts := &Options{}
	options.Resolve(opts, flagSet, cfg)
	fmt.Printf("sendtopic:%s\n", opts.SendTopic)

	// 创建Logger
	logger.SetConsole(true)
	logger.SetRollingFile(opts.LogDir, opts.LogFileName, opts.LogNum, opts.LogFileSize, logger.MB)
	logger.SetLevel(logger.INFO)

	logger.Warn("begin ...")
	// main start
	var wg sync.WaitGroup
	// check g status
	wg.Add(1)
	go func() {
		checkGStatus(opts)
		wg.Done()
	}()

	// 等待状态位的设置
	time.Sleep(time.Second * 2)

	// 启动客户端发送
	go sendto_g(opts)

	// 创建监听
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.ParseIP(opts.ListenIP),
		Port: opts.ListenPort,
	})
	if err != nil {
		fmt.Println("监听失败!", err)
		return
	}
	defer socket.Close()

	// 日志输出
	go func() {
		Stated()
	}()

	// 创建nsq生产者
	createProducer(opts)

	for {
		// 读取数据
		data := make([]byte, 4096)
		dataCount, _, err := socket.ReadFromUDP(data)
		//		buff := make([]byte, dataCount)
		//		copy(buff[:], data)
		if err != nil {
			fmt.Println("读取数据失败!", err)
			continue
		}

		// 将数据放入通道
		datachannel <- data[:dataCount]

		atomic.AddInt64(&TotalRecvCount, 1)
	}

	wg.Wait()
	logger.Warn("finish ...")
}

func createProducer(opts *Options) {
	// nsqservers
	nsqServersLen := len(opts.Nsqservers)
	var producerPtr []*nsq.Producer

	for _, nsqServer := range opts.Nsqservers {
		p, err := nsq.NewProducer(nsqServer, nsq.NewConfig())
		if err != nil {
			panic(err)
		}

		producerPtr = append(producerPtr, p)
	}

	for j := 0; j < runtime.GOMAXPROCS(4); j++ {
		//	for j := 0; j < runtime.GOMAXPROCS(runtime.NumCPU())-2; j++ {
		p := producerPtr[rand.Intn(nsqServersLen)]
		go func() {
			publish(p, opts.SendTopic)
		}()
	}
}

// nsq 发布消息
func publish(producer *nsq.Producer, send_topic string) {
	defer producer.Stop()
	for {
		data := <-nsqchannel
		if err := producer.Publish(send_topic, []byte(data)); err != nil {
			// 将数据放回nsqchannel
			nsqchannel <- data
			// 等待2s再取数据
			time.Sleep(time.Second * 2)
		}
	}
}

// g
func httpGetJson(url string) (*GJson, error) {
	// 超时设置
	c := http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				deadline := time.Now().Add(1 * time.Second)
				c, err := net.DialTimeout(netw, addr, time.Second*1)
				if err != nil {
					return nil, err
				}
				c.SetDeadline(deadline)
				return c, nil
			},
		},
	}

	//	resp, err := http.Get(url)
	resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	input, err := ioutil.ReadAll(resp.Body)
	var g GJson
	json.Unmarshal(input, &g)
	return &g, nil
}
func setGStatus(opts *Options) error {
	var url string = opts.HttpUrl
	g, err := httpGetJson(url)
	if err != nil {
		atomic.StoreUint32(&gStatus, uint32(0))
		return nil
	}

	if g.Master == true {
		atomic.StoreUint32(&gStatus, uint32(1))
		return nil
	} else if g.Slave == true {
		atomic.StoreUint32(&gStatus, uint32(2))
		return nil
	} else {
		atomic.StoreUint32(&gStatus, uint32(0))
		return nil
	}
}
func checkGStatus(opts *Options) {
	timer := time.NewTicker(time.Duration(opts.TimeInterval) * time.Second)
	err := setGStatus(opts)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	for {
		select {
		case <-timer.C:
			err = setGStatus(opts)
			if err != nil {
				logger.Error(err.Error())
				return
			}
		}
	}
}

func sendto_g(opts *Options) {
	conn, err := net.Dial("udp", opts.ServerAddr)
	if err != nil {
		fmt.Println(err)
		logger.Error(err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	sendData := make([]byte, 2048)
	copy(sendData, opts.MagicCode)

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.BigEndian, opts.MagicIP)
	if err != nil {
		logger.Error(err.Error())
		panic(err)
	}

	copy(sendData[4:], buf.Bytes())

	for {
		data := <-datachannel
		if gStatus == 1 || gStatus == 2 {
			dataLen := len(data)
			copy(sendData[8:], data)
			conn.Write(sendData[:8+dataLen])
		} else {
			// 数据生产到nsq
			nsqchannel <- data
		}
	}
}
