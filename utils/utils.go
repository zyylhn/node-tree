package utils

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"golang.org/x/text/encoding/simplifiedchinese"
)

func GenerateUUID() string {
	u2, _ := uuid.NewV4()
	uu := strings.Replace(u2.String(), "-", "", -1)
	uuidRe := uu[11:21]
	return uuidRe
}

func GetStringMd5(s string) string {
	md5Data := md5.New()
	md5Data.Write([]byte(s))
	md5Str := hex.EncodeToString(md5Data.Sum(nil))
	return md5Str
}

// StringSliceReverse 逆置字符串
func StringSliceReverse(src []string) {
	if src == nil {
		return
	}
	count := len(src)
	mid := count / 2
	for i := 0; i < mid; i++ {
		tmp := src[i]
		src[i] = src[count-1]
		src[count-1] = tmp
		count--
	}
}

func Str2Int(str string) (int, error) {
	num, err := strconv.ParseInt(str, 10, 32)
	return int(uint32(num)), err
}

func Int2Str(num int) string {
	b := strconv.Itoa(num)
	return b
}

func CheckSystem() (sysType uint32) {
	var cos = runtime.GOOS
	switch cos {
	case "windows":
		sysType = 0x01
	case "linux":
		sysType = 0x02
	default:
		sysType = 0x03
	}
	return
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

// CheckIPPort 检查和拆分地址，返回指定ip地址端口和本地所有地址的端口（192.168.1.100:8000,0.0.0.0:8000）
func CheckIPPort(info string) (normalAddr string, reuseAddr string, err error) {
	var (
		readyIP   string
		readyPort int
	)

	splintedInfo := strings.Split(info, ":")

	if len(splintedInfo) == 1 {
		readyIP = "0.0.0.0"
		readyPort, err = strconv.Atoi(info)
	} else if len(splintedInfo) == 2 {
		readyIP = splintedInfo[0]
		readyPort, err = strconv.Atoi(splintedInfo[1])
	} else {
		err = errors.New("please input either port(1~65535) or ip:port(1-65535)")
		return
	}

	if err != nil || readyPort < 1 || readyPort > 65535 || readyIP == "" {
		err = errors.New("please input either port(1~65535) or ip:port(1-65535)")
		return
	}

	normalAddr = readyIP + ":" + strconv.Itoa(readyPort)
	reuseAddr = "0.0.0.0:" + strconv.Itoa(readyPort)

	return
}

func SplitIpPort(info string) (ip string, port int, err error) {

	splintedInfo := strings.Split(info, ":")

	if len(splintedInfo) == 1 {
		ip = "0.0.0.0"
		port, err = strconv.Atoi(info)
		if err != nil {
			return
		}
	} else if len(splintedInfo) == 2 {
		ip = splintedInfo[0]
		port, err = strconv.Atoi(splintedInfo[1])
		if err != nil {
			return
		}
	} else {
		err = errors.New("please input either port(1~65535) or ip:port(1-65535)")
		return
	}

	if port < 1 || port > 65535 || ip == "" {
		err = errors.New("please input either port(1~65535) or ip:port(1-65535)")
		return
	}

	return
}

func GetBackWardAddr(info string) (addr string, err error) {
	var (
		readyIP   string
		readyPort int
	)

	splintedInfo := strings.Split(info, ":")

	if len(splintedInfo) == 1 {
		readyIP = "127.0.0.1"
		readyPort, err = strconv.Atoi(info)
	} else if len(splintedInfo) == 2 {
		readyIP = splintedInfo[0]
		readyPort, err = strconv.Atoi(splintedInfo[1])
	} else {
		err = errors.New("please input either port(1~65535) or ip:port(1-65535): " + info)
		return
	}

	if err != nil || readyPort < 1 || readyPort > 65535 || readyIP == "" {
		err = errors.New("please input either port(1~65535) or ip:port(1-65535): " + info)
		return
	}

	addr = net.JoinHostPort(readyIP, strconv.Itoa(readyPort))

	return
}

func CheckIfIP4(ip string) bool {
	for i := 0; i < len(ip); i++ {
		switch ip[i] {
		case '.':
			return true
		case ':':
			return false
		}
	}
	return false
}

// CheckRange 从小到大排序
func CheckRange(nodes []int) {
	for m := len(nodes) - 1; m > 0; m-- {
		var flag = false
		for n := 0; n < m; n++ {
			if nodes[n] > nodes[n+1] {
				temp := nodes[n]
				nodes[n] = nodes[n+1]
				nodes[n+1] = temp
				flag = true
			}
		}
		if !flag {
			break
		}
	}
}

// GetDigitLen 获取int为几位数
func GetDigitLen(num int) int {
	var length int
	for {
		num = num / 10
		if num != 0 {
			length++
		} else {
			length++
			return length
		}
	}
}

// GetRandomString 获取指定长度到的随机字符串
func GetRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	var result []byte
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

// GetRandomInt 获取指定数字之下的随机数
func GetRandomInt(max int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(max)
}

// ConvertStr2GBK gbk编码
func ConvertStr2GBK(str string) string {
	ret, err := simplifiedchinese.GBK.NewEncoder().String(str)
	if err != nil {
		ret = str
	}
	return ret
}

// ConvertGBK2Str gbk解码
func ConvertGBK2Str(gbkStr string) string {
	ret, err := simplifiedchinese.GBK.NewDecoder().String(gbkStr)
	if err != nil {
		ret = gbkStr
	}
	return ret

}

func PrintJson(data interface{}) string {
	// 尝试将数据序列化为 JSON
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		// 如果发生错误，直接 panic，并输出原始数据
		panic(fmt.Sprintf("Failed to serialize data: %v\nError: %v", data, err))
	}
	// 输出成功序列化的 JSON 数据
	return string(jsonData)
}
