//go:build windows

package authfileguard

import (
	"errors"
	"io/fs"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsFileRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

func renameFileNoReplace(root *os.Root, stagedName, targetName string) (cleanupWarning error, err error) {
	directory, errOpenDirectory := root.Open(".")
	if errOpenDirectory != nil {
		return nil, errOpenDirectory
	}
	directoryHandle := windows.Handle(directory.Fd())

	objectName, errObjectName := windows.NewNTUnicodeString(stagedName)
	if errObjectName != nil {
		return nil, errors.Join(errObjectName, directory.Close())
	}
	attributes := windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: directoryHandle,
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}
	var stagedHandle windows.Handle
	var openStatus windows.IO_STATUS_BLOCK
	var allocationSize int64
	if errOpen := windows.NtCreateFile(
		&stagedHandle,
		windows.DELETE|windows.SYNCHRONIZE,
		&attributes,
		&openStatus,
		&allocationSize,
		0,
		windows.FILE_SHARE_DELETE|windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_OPEN,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	); errOpen != nil {
		return nil, errors.Join(errOpen, directory.Close())
	}

	targetUTF16, errTarget := windows.UTF16FromString(targetName)
	if errTarget != nil {
		return nil, errors.Join(errTarget, windows.CloseHandle(stagedHandle), directory.Close())
	}
	targetUTF16 = targetUTF16[:len(targetUTF16)-1]
	var layout windowsFileRenameInformation
	nameOffset := int(unsafe.Offsetof(layout.FileName))
	buffer := make([]byte, nameOffset+len(targetUTF16)*2)
	info := (*windowsFileRenameInformation)(unsafe.Pointer(&buffer[0]))
	info.RootDirectory = directoryHandle
	info.FileNameLength = uint32(len(targetUTF16) * 2)
	copy(unsafe.Slice(&info.FileName[0], len(targetUTF16)), targetUTF16)

	var renameStatus windows.IO_STATUS_BLOCK
	errRename := windows.NtSetInformationFile(
		stagedHandle,
		&renameStatus,
		&buffer[0],
		uint32(len(buffer)),
		windows.FileRenameInformation,
	)
	errClose := errors.Join(windows.CloseHandle(stagedHandle), directory.Close())
	if errRename != nil {
		if errors.Is(errRename, windows.STATUS_OBJECT_NAME_COLLISION) ||
			errors.Is(errRename, windows.ERROR_ALREADY_EXISTS) ||
			errors.Is(errRename, windows.ERROR_FILE_EXISTS) {
			errRename = fs.ErrExist
		}
		return nil, errors.Join(errRename, errClose)
	}
	return errClose, nil
}
