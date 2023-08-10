//go:build darwin || dragonfly || freebsd || linux || nacl || netbsd || openbsd || solaris

package rpc

/*
#include <sys/un.h>

int max_socket_path_size_v2() {
struct sockaddr_un s;
return sizeof(s.sun_path);
}
*/
import "C"

var (
	max_path_size = C.max_socket_path_size_v2()
)
