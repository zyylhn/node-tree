//go:build windows

package system_info

import (
	"fmt"
	"golang.org/x/sys/windows"
	"unsafe"
)

var ModKernel32 = windows.NewLazySystemDLL("kernel32.dll")

var (
	procGetNativeSystemInfo = ModKernel32.NewProc("GetNativeSystemInfo")
)

type systemInfo struct {
	wProcessorArchitecture      uint16
	wReserved                   uint16
	dwPageSize                  uint32
	lpMinimumApplicationAddress uintptr
	lpMaximumApplicationAddress uintptr
	dwActiveProcessorMask       uintptr
	dwNumberOfProcessors        uint32
	dwProcessorType             uint32
	dwAllocationGranularity     uint32
	wProcessorLevel             uint16
	wProcessorRevision          uint16
}

func KernelArch() (string, error) {
	var system systemInfo
	procGetNativeSystemInfo.Call(uintptr(unsafe.Pointer(&system)))

	const (
		PROCESSOR_ARCHITECTURE_INTEL = 0
		PROCESSOR_ARCHITECTURE_ARM   = 5
		PROCESSOR_ARCHITECTURE_ARM64 = 12
		PROCESSOR_ARCHITECTURE_IA64  = 6
		PROCESSOR_ARCHITECTURE_AMD64 = 9
	)
	switch system.wProcessorArchitecture {
	case PROCESSOR_ARCHITECTURE_INTEL:
		if system.wProcessorLevel < 3 {
			return "i386", nil
		}
		if system.wProcessorLevel > 6 {
			return "i686", nil
		}
		return fmt.Sprintf("i%d86", system.wProcessorLevel), nil
	case PROCESSOR_ARCHITECTURE_ARM:
		return "arm", nil
	case PROCESSOR_ARCHITECTURE_ARM64:
		return "aarch64", nil
	case PROCESSOR_ARCHITECTURE_IA64:
		return "ia64", nil
	case PROCESSOR_ARCHITECTURE_AMD64:
		return "x86_64", nil
	}
	return "", nil
}
