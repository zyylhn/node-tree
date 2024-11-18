package admin

import (
	"context"
	"fmt"
	"github.com/kataras/golog"
	"github.com/zyylhn/node-tree/admin/initial"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/utils"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

//--------------------发现的问题-------------------------
//todo 每个功能节点功能的run，添加了ctx参数，但是当ctx取消后，继续调用其功能会发生阻塞。这块需要考虑一下怎么处理。由此也引出一个任务就是整个节点管理系统的停止和资源回收（manager的停止运行并停止服务，channel的关闭）

// todo 测试反向端口转发和远程加载

// admin与agent的连接测试
func TestAdminConnectAgent(t *testing.T) {
	admin := NewAdmin(&initial.Options{
		Mode:       0,
		Secret:     "default",
		Listen:     "",
		Connect:    "127.0.0.1:7001",
		Proxy:      "",
		ProxyU:     "",
		ProxyP:     "",
		Downstream: "raw",
	}, golog.New())
	re := make(chan interface{}, 10)
	go func() {
		readMessage(re)
	}()
	fmt.Println(startAgent("-l", "127.0.0.1:7001", "-s", "default"))
	time.Sleep(1 * time.Second)

	err := admin.Start(context.Background(), re)
	if err != nil {
		fmt.Println(err)
		return
	}
	time.Sleep(3 * time.Second)
	admin.CloseWithAllNode(false)
}

// 使用agent0主动连接新节点
func TestAgentConnectAgent(t *testing.T) {
	admin := NewAdmin(&initial.Options{
		Mode:       0,
		Secret:     "default",
		Listen:     "",
		Connect:    "127.0.0.1:7001",
		Proxy:      "",
		ProxyU:     "",
		ProxyP:     "",
		Downstream: "raw",
	}, golog.New())
	re := make(chan interface{}, 10)
	go func() {
		readMessage(re)
	}()
	fmt.Println(startAgent("-l", "127.0.0.1:7001", "-s", "default"))
	fmt.Println(startAgent("-l", "127.0.0.1:7002", "-s", "default"))
	time.Sleep(1 * time.Second)

	err := admin.Start(context.Background(), re)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(admin.ConnectToNewAgent(admin.Topology.GetUUID(0), "127.0.0.1:7002", "", "", ""))

	time.Sleep(3 * time.Second)
	admin.CloseWithAllNode(false)
}

// 使用agent0被动连接新节点并获取新增节点的节点ID
func TestAgentListenerReturnID(t *testing.T) {
	adminAgentConnectAddr := "127.0.0.1:7001"
	listenerAddr := "127.0.0.1:7003"
	admin := NewAdmin(&initial.Options{
		Mode:       0,
		Secret:     "default",
		Listen:     "",
		Connect:    adminAgentConnectAddr,
		Proxy:      "",
		ProxyU:     "",
		ProxyP:     "",
		Downstream: "raw",
	}, golog.New())
	re := make(chan interface{}, 10)
	go func() {
		readMessage(re)
	}()
	fmt.Println(startAgent("-l", adminAgentConnectAddr, "-s", "default"))
	fmt.Println(startAgent("-c", listenerAddr, "-s", "default", "-r", "1")) //每秒重连一次
	time.Sleep(1 * time.Second)

	err := admin.Start(context.Background(), re)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(admin.CreateWaitOnLineListener(context.Background(), admin.Topology.GetUUID(0), listenerAddr))

	time.Sleep(3 * time.Second)
	admin.CloseWithAllNode(false)
}

// 使用agent0创建监听器并测试监听器的管理功能和是否能正常上线
func TestAgentListener(t *testing.T) {
	adminAgentConnectAddr := "127.0.0.1:7001"
	listenerAddr := "127.0.0.1:7003"
	admin := NewAdmin(&initial.Options{
		Mode:       0,
		Secret:     "default",
		Listen:     "",
		Connect:    adminAgentConnectAddr,
		Proxy:      "",
		ProxyU:     "",
		ProxyP:     "",
		Downstream: "raw",
	}, golog.New())
	re := make(chan interface{}, 10)
	go func() {
		readMessage(re)
	}()
	fmt.Println(startAgent("-l", adminAgentConnectAddr, "-s", "default"))
	time.Sleep(1 * time.Second)

	err := admin.Start(context.Background(), re)
	if err != nil {
		fmt.Println(err)
		return
	}
	time.Sleep(time.Second)
	fmt.Println("新建监听器")
	fmt.Println(admin.CreateListener(admin.Topology.GetUUID(0), false, listenerAddr))
	fmt.Println("查询监听器")
	fmt.Println(admin.GetListener(admin.Topology.GetUUID(0)))

	fmt.Println("启动agent")
	fmt.Println(startAgent("-c", listenerAddr, "-s", "default", "-r", "1")) //每秒重连一次
	time.Sleep(2 * time.Second)
	fmt.Println("停止监听器")
	fmt.Println(admin.StopListener(admin.Topology.GetUUID(0), "all"))
	fmt.Println("查询监听器")
	fmt.Println(admin.GetListener(admin.Topology.GetUUID(0)))

	time.Sleep(3 * time.Second)
	admin.CloseWithAllNode(false)
}

// 使用agent0创建监听器并测试监听器自动停止功能
func TestAgentListenerAutoStop(t *testing.T) {
	adminAgentConnectAddr := "127.0.0.1:7001"
	listenerAddr := "127.0.0.1:7003"
	admin := NewAdmin(&initial.Options{
		Mode:       0,
		Secret:     "default",
		Listen:     "",
		Connect:    adminAgentConnectAddr,
		Proxy:      "",
		ProxyU:     "",
		ProxyP:     "",
		Downstream: "raw",
	}, golog.New())
	re := make(chan interface{}, 10)
	go func() {
		readMessage(re)
	}()
	fmt.Println(startAgent("-l", adminAgentConnectAddr, "-s", "default"))
	time.Sleep(time.Second)
	err := admin.Start(context.Background(), re)
	if err != nil {
		fmt.Println(err)
		return
	}
	time.Sleep(time.Second)
	fmt.Println("新建监听器")
	fmt.Println(admin.CreateListener(admin.Topology.GetUUID(0), true, listenerAddr))
	fmt.Println("查询监听器")
	fmt.Println(admin.GetListener(admin.Topology.GetUUID(0)))

	fmt.Println("启动agent")
	fmt.Println(startAgent("-c", listenerAddr, "-s", "default", "-r", "1")) //每秒重连一次
	time.Sleep(2 * time.Second)
	fmt.Println("查询监听器")
	fmt.Println(admin.GetListener(admin.Topology.GetUUID(0)))

	time.Sleep(3 * time.Second)
	admin.CloseWithAllNode(false)
}

//ssh tunnel方式连接

//==============================================backward test==============================================

// 测试反向端口转发基础功能
func TestBackWard(t *testing.T) {
	adminAgentConnectAddr := "127.0.0.1:7001"
	serverAddr := "127.0.0.1:7005"
	backWardAddr := "127.0.0.1:7006"
	admin := NewAdmin(&initial.Options{
		Mode:       0,
		Secret:     "default",
		Listen:     "",
		Connect:    adminAgentConnectAddr,
		Proxy:      "",
		ProxyU:     "",
		ProxyP:     "",
		Downstream: "raw",
	}, golog.New())
	re := make(chan interface{}, 10)
	go func() {
		readMessage(re)
	}()
	fmt.Println(startAgent("-l", adminAgentConnectAddr, "-s", "default"))
	time.Sleep(1 * time.Second)
	err := admin.Start(context.Background(), re)
	if err != nil {
		fmt.Println(err)
		return
	}
	resultChan := make(chan string, 10)
	//启动tcp监听器
	go func() {
		for data := range resultChan {
			fmt.Println("tcpListen 接收到数据：", data)
		}
	}()
	go func() {
		err = tcpListen(serverAddr, resultChan)
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(2 * time.Second)
	fmt.Println("创建反向端口转发")
	fmt.Println(admin.CreateBackward(serverAddr, "7006", admin.Topology.GetUUID(0)))

	err = writeMessage(backWardAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	time.Sleep(3 * time.Second)
	admin.CloseWithAllNode(false)
}

//测试关闭查询功能

//===============================================remote load===============================================

// 远程加载基本功能
func TestRemoteLoad(t *testing.T) {
	admin := NewAdmin(&initial.Options{
		Mode:       0,
		Secret:     "default",
		Listen:     "",
		Connect:    "127.0.0.1:7001",
		Proxy:      "",
		ProxyU:     "",
		ProxyP:     "",
		Downstream: "raw",
	}, golog.New().SetLevel(golog.DebugLevel.String()))
	re := make(chan interface{}, 10)
	go func() {
		readMessage(re)
	}()
	fmt.Println(startAgent("-l", "127.0.0.1:7001", "-s", "default"))
	time.Sleep(1 * time.Second)

	err := admin.Start(context.Background(), re)
	if err != nil {
		fmt.Println(err)
		return
	}

	module, err := os.Open("./testData/printExec/printExec")
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	remote, err := admin.CreateRemoteLoad(module, "asdasdas", "print exec", &ctx, &cancel)
	if err != nil {
		panic(err)
	}
	result, err := remote.LoadExec(admin.Topology.GetUUID(0), "")
	fmt.Println(string(result))

	time.Sleep(3 * time.Second)
	admin.CloseWithAllNode(false)
}

// 连接指定地址并写入测试数据
func writeMessage(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		conn.Write([]byte(fmt.Sprintf("第%d次写入数据", i)))
	}
	conn.Close()
	return nil
}

func tcpListen(addr string, resultChan chan string) error {
	defer close(resultChan)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	data := make([]byte, 1024)
	for {
		n, err := conn.Read(data)
		if err != nil {
			return err
		}
		resultChan <- string(data[:n])
	}
}

// startLocalProgram 启动指定路径的本地程序
func startAgent(args ...string) (*os.Process, error) {
	// 创建命令并传递参数
	cmd := exec.Command("../output/cmd", args...)

	// 可选设置：设置启动进程的 SysProcAttr 参数（适用于 Windows 隐藏窗口的需求）
	// 启动程序
	err := cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start program: %v", err)
	}

	// 返回进程信息
	fmt.Printf("Program started with PID %d\n", cmd.Process.Pid)
	return cmd.Process, nil
}

func readMessage(ch chan interface{}) {
	for re := range ch {
		switch m := re.(type) {
		case *topology.NodeJoin:
			fmt.Printf("Node join %v\n", utils.PrintJson(m))
		case *topology.NodeReOnline:
			fmt.Printf("Node reonline %s\n", utils.PrintJson(m))
		case *topology.NodeUpdate:
			fmt.Printf("Node update %s\n", utils.PrintJson(m))
		}
	}
}
