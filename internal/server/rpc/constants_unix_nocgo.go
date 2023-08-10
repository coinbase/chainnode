//go:build !cgo && !windows
// +build !cgo,!windows

package rpc

var (
	//  On Linux, sun_path is 108 bytes in size
	// see http://man7.org/linux/man-pages/man7/unix.7.html
	max_path_size = 108
)
