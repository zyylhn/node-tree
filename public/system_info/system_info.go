package system_info

import (
	"encoding/json"
	"net"
	"os"
	"os/user"
	"regexp"
	"runtime"
)

type SystemInfo struct {
	OsVersion       *OsVersion   `json:"os_version"`         //操作系统信息
	CpuNum          int          `json:"cpu_num"`            //目标的cpu个数
	OsArch          string       `json:"os_arch"`            //操作系统架构
	ProgramArch     string       `json:"program_arch"`       //程序架构
	NetWorkCardInfo NetWorkCards `json:"net_work_card_info"` //网卡信息
	HostName        string       `json:"host_name"`          //主机名
	AgentProcess    *Process     `json:"agent_process"`      //当前进程信息
	User            *user.User   `json:"user"`               //当前用户
	IsDocker        bool         `json:"is_docker"`          //当前环境是否为docker
	AgentPath       string       `json:"agent_path"`
}

func NewSystemInfo() *SystemInfo {
	info := new(SystemInfo)
	info.OsVersion = newOsVersion()
	info.ProgramArch = runtime.GOARCH
	info.OsArch, _ = KernelArch()
	info.CpuNum = runtime.NumCPU()
	info.NetWorkCardInfo, _ = newNetWorkCards()
	info.HostName, _ = os.Hostname()
	info.AgentProcess = newProcess()
	info.User, _ = user.Current()
	info.IsDocker = IsDocker()
	info.AgentPath, _ = os.Executable()
	return info
}

func (s *SystemInfo) String() string {
	if s != nil {
		b, _ := json.Marshal(s)
		return string(b)
	}
	return ""
}

type OsVersion struct {
	OS            string `json:"os"`             //linux or windows...
	Version       string `json:"version"`        //kali、ubuntu、Windows 10...
	VersionNumber string `json:"version_number"` //6.1.7601...
}

type Process struct {
	Pid  int `json:"pid"`
	PPid int `json:"p_pid"`
}

func newProcess() *Process {
	pid := os.Getpid()
	ppid := os.Getppid()
	return &Process{Pid: pid, PPid: ppid}
}

type NetWorkCard struct {
	IPAddr net.IP     `json:"ip_addr"`
	IPMask net.IPMask `json:"ip_mask"`
}

type NetWorkCards []*NetWorkCard

// 获取本机所有网卡地址及子网掩码
func newNetWorkCards() ([]*NetWorkCard, error) {
	var re []*NetWorkCard
	addr, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, address := range addr {
		// 检查ip地址判断是否回环地址
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				re = append(re, &NetWorkCard{IPAddr: ipNet.IP, IPMask: ipNet.Mask})
			}
		}
	}
	return re, nil
}

func IsDocker() bool {
	//查看跟目录是否存在.docker env
	_, err := os.Stat("/.dockerenv")
	if err == nil {
		return true
	}
	if os.IsExist(err) {
		return true
	}
	file, err := os.ReadFile("/proc/1/cgroup")
	if err != nil {
		return false
	}
	return regexp.MustCompile(`\d+?:\w+?:/docker/[0-9a-zA-Z]*?`).Match(file)
}
