//go:build !darwin && !linux && !freebsd && !openbsd && !netbsd && !solaris && !windows

package system_info

import "errors"

func KernelArch() (string, error) {
	return "", errors.New("Unrecognized (unsupported operating system)")
}
