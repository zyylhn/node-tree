package public

import (
	"encoding/binary"
	"errors"
	"github.com/zyylhn/node-tree/utils"
	"io"
	"net"
	"strconv"
)

// Proxy 使用代理去连接目标
type Proxy struct {
	PeerAddr  string
	ProxyAddr string
	UserName  string
	Password  string
}

// NewProxy peer addr为要链接的目标，proxy为代理
func NewProxy(peerAddr, proxyAddr, username, password string) *Proxy {
	proxy := new(Proxy)
	proxy.PeerAddr = peerAddr
	proxy.ProxyAddr = proxyAddr
	proxy.UserName = username
	proxy.Password = password
	return proxy
}

func (proxy *Proxy) Dial() (net.Conn, error) {
	var notSupport = errors.New("unknown protocol")
	var wrongAuth = errors.New("wrong auth method")
	var serverErr = errors.New("proxy server error")
	var tooLong = errors.New("user/pass too long(max 255)")
	var authFail = errors.New("wrong username/password")

	proxyConn, err := net.Dial("tcp", proxy.ProxyAddr)
	if err != nil {
		return proxyConn, err
	}

	host, portS, err := net.SplitHostPort(proxy.PeerAddr)
	if err != nil {
		return proxyConn, err
	}
	portUint64, err := strconv.ParseUint(portS, 10, 16)
	if err != nil {
		return proxyConn, err
	}

	port := uint16(portUint64)
	portB := make([]byte, 2)
	binary.BigEndian.PutUint16(portB, port)
	// No Auth
	if proxy.UserName == "" && proxy.Password == "" {
		_, _ = proxyConn.Write([]byte{0x05, 0x01, 0x00})
	} else {
		// u and p
		_, _ = proxyConn.Write([]byte{0x05, 0x01, 0x02})
	}

	authWayBuf := make([]byte, 2)

	_, err = io.ReadFull(proxyConn, authWayBuf)
	if err != nil {
		return proxyConn, err
	}

	if authWayBuf[0] == 0x05 {
		switch authWayBuf[1] {
		case 0x00:
		case 0x02:
			userLen := len(proxy.UserName)
			passLen := len(proxy.Password)
			if userLen > 255 || passLen > 255 {
				return proxyConn, tooLong
			}

			buff := make([]byte, 0, 3+userLen+passLen)
			buff = append(buff, 0x01, byte(userLen))
			buff = append(buff, []byte(proxy.UserName)...)
			buff = append(buff, byte(passLen))
			buff = append(buff, []byte(proxy.Password)...)
			_, _ = proxyConn.Write(buff)

			responseBuf := make([]byte, 2)
			_, err = io.ReadFull(proxyConn, responseBuf)
			if err != nil {
				return proxyConn, err
			}

			if responseBuf[0] == 0x01 {
				if responseBuf[1] == 0x00 {
					break
				} else {
					return proxyConn, authFail
				}
			} else {
				return proxyConn, notSupport
			}
		case 0xff:
			return proxyConn, wrongAuth
		default:
			return proxyConn, notSupport
		}

		var (
			buff []byte
			ip   net.IP
		)
		isV4 := utils.CheckIfIP4(host)
		if isV4 {
			buff = make([]byte, 0, 10)
			ip = net.ParseIP(host).To4()
			buff = append(buff, []byte{0x05, 0x01, 0x00, 0x01}...)
		} else {
			buff = make([]byte, 0, 22)
			ip = net.ParseIP(host).To16()
			buff = append(buff, []byte{0x05, 0x01, 0x00, 0x04}...)
		}
		buff = append(buff, []byte(ip)...)
		buff = append(buff, portB...)
		_, _ = proxyConn.Write(buff)

		respBuf := make([]byte, 4)
		_, err = io.ReadFull(proxyConn, respBuf)
		if err != nil {
			return proxyConn, err
		}
		if respBuf[0] == 0x05 {
			if respBuf[1] != 0x00 {
				return proxyConn, serverErr
			}
			switch respBuf[3] {
			case 0x01:
				resultBuf := make([]byte, 6)
				_, err = io.ReadFull(proxyConn, resultBuf)
			case 0x04:
				resultBuf := make([]byte, 18)
				_, err = io.ReadFull(proxyConn, resultBuf)
			default:
				return proxyConn, notSupport
			}
			if err != nil {
				return proxyConn, err
			}

			return proxyConn, nil
		} else {
			return proxyConn, notSupport
		}
	} else {
		return proxyConn, notSupport
	}
}
