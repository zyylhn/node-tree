package public

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/zyylhn/node-tree/utils"
)

// ActivePreAuth 主动身份验证，先给目标发送密钥，然后接受返回的密钥做验证
func ActivePreAuth(conn net.Conn, key string) error {
	var passErr = errors.New("invalid secret, check the secret")

	defer func(conn net.Conn, t time.Time) {
		_ = conn.SetReadDeadline(t)
	}(conn, time.Time{})
	err := conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return err
	}

	secret := utils.GetStringMd5(key)
	_, err = conn.Write([]byte(secret[:16]))
	if err != nil {
		return err
	}
	buffer := make([]byte, 16)
	count, err := io.ReadFull(conn, buffer)

	var timeoutErr net.Error
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		_ = conn.Close()
		return passErr
	}

	if err != nil {
		_ = conn.Close()
		return passErr
	}

	if string(buffer[:count]) == secret[:16] {
		return nil
	}

	_ = conn.Close()

	return passErr
}

// PassivePreAuth 被动身份验证，接受别人发送的密钥做验证，然后在返密钥给别人
func PassivePreAuth(conn net.Conn, key string) error {
	var passErr = errors.New("invalid secret, check the secret")

	defer func(conn net.Conn, t time.Time) {
		_ = conn.SetReadDeadline(t)
	}(conn, time.Time{})
	err := conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return err
	}

	secret := utils.GetStringMd5(key)

	buffer := make([]byte, 16)
	count, err := io.ReadFull(conn, buffer)

	var timeoutErr net.Error
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		_ = conn.Close()
		return passErr
	}

	if err != nil {
		_ = conn.Close()
		return passErr
	}

	if string(buffer[:count]) == secret[:16] {
		_, _ = conn.Write([]byte(secret[:16]))
		return nil
	}

	_ = conn.Close()

	return passErr
}
