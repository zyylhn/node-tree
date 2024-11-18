//go:build linux || freebsd || openbsd || netbsd || darwin || solaris

package system_info

import "golang.org/x/sys/unix"

func KernelArch() (string, error) {
	var utsName unix.Utsname
	err := unix.Uname(&utsName)
	if err != nil {
		return "", err
	}
	return unix.ByteSliceToString(utsName.Machine[:]), nil
}
