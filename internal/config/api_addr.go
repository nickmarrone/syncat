package config

import (
	"fmt"
	"net"
)

// The API is loopback-only (SPEC.md §8: it isn't exposed on the LAN even if
// the token leaks). The rule lives here, not in internal/api, so that
// Validate can refuse an api_addr the daemon would refuse to bind, and so
// internal/api's listener and the CLI's client apply the very same rule.

// NormalizeAPIAddr checks that addr is a loopback host:port and returns it
// as the authority to bind and dial. An empty host (":8347") means
// 127.0.0.1, rather than "all interfaces" as net.Listen would read it.
// The host may be "localhost" or any loopback IP, v4 or v6.
func NormalizeAPIAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("invalid API address %q: %w (want a loopback host:port, e.g. %s)", addr, err, DefaultAPIAddr)
	}
	if port == "" {
		return "", fmt.Errorf("invalid API address %q: port is empty (want a loopback host:port, e.g. %s)", addr, DefaultAPIAddr)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	if !IsLoopbackHost(host) {
		return "", fmt.Errorf("refusing non-loopback API address %q: the API is loopback-only (localhost, 127.0.0.1 or [::1])", addr)
	}
	return net.JoinHostPort(host, port), nil
}

// IsLoopbackHost reports whether host, a bare host already split from its
// port, is the literal "localhost" or a loopback IP. A user configuring
// api_addr = "localhost:8347" means loopback and should be allowed to bind.
func IsLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
