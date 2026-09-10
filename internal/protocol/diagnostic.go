package protocol

import (
	"strings"
	"unicode"
)

const MaxDiagnosticRunes = 512

// SanitizeDiagnostic makes untrusted wire text safe for one-line logs,
// terminals, status JSON, and CLI rendering.
func SanitizeDiagnostic(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	runes := []rune(s)
	if len(runes) > MaxDiagnosticRunes {
		s = string(runes[:MaxDiagnosticRunes]) + "…"
	}
	return s
}

func RedactDiagnostic(s string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[redacted]")
		}
	}
	return SanitizeDiagnostic(s)
}
