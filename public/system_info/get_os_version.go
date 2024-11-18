//go:build !windows

package system_info

import (
	"github.com/zyylhn/node-tree/utils"
	"io"
	"os/exec"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"syscall"
)

func newOsVersion() *OsVersion {
	osVersion := new(OsVersion)
	osVersion.OS = runtime.GOOS
	switch osVersion.OS {
	case "linux":
		if ok, _ := utils.PathExists("/etc/os-release"); ok {
			cmd := exec.Command("cat", "/etc/os-release")
			stdout, _ := cmd.StdoutPipe()
			cmd.SysProcAttr = &syscall.SysProcAttr{Foreground: true}
			signal.Ignore(syscall.SIGTTIN, syscall.SIGTTOU)
			err := cmd.Start()
			if err != nil {
				return nil
			}
			content, err := io.ReadAll(stdout)
			if err == nil {
				id := regexp.MustCompile(`\nID="?(.*?)"?\n`).FindStringSubmatch(string(content))
				if len(id) > 1 {
					osVersion.Version = id[1]
				}

				versionId := regexp.MustCompile(`VERSION_ID="?([.0-9]+)"?\n`).FindStringSubmatch(string(content))
				if len(versionId) > 1 {
					osVersion.VersionNumber = versionId[1]
				}
			}
		}
	case "windows":
		cmd := exec.Command("cmd.exe", "/c", "systeminfo")
		content, err := cmd.Output()
		if err != nil {
			break
		}
		versionNumRe := regexp.MustCompile(`OS .*:[\s\S]* ([0-9]+\.[0-9]\.+[0-9]*)`).FindStringSubmatch(string(content))
		if len(versionNumRe) > 1 {
			osVersion.VersionNumber = versionNumRe[1]
		}

		versionRe := regexp.MustCompile(`OS .*:[\s\S]* (Microsoft Windows .*)`).FindStringSubmatch(string(content))
		if len(versionRe) > 1 {
			osVersion.Version = strings.TrimSpace(versionRe[1])
		}
	default:
	}
	return osVersion
}
