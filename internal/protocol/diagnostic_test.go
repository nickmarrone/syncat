package protocol

import (
	"strings"
	"testing"
)

func TestRedactDiagnosticIsSingleLineBoundedAndCredentialFree(t *testing.T) {
	secret := "tc://distinctive-private-credential"
	in := "failure at " + secret + "\nforged log\x1b[31m" + strings.Repeat("x", MaxDiagnosticRunes)
	got := RedactDiagnostic(in, secret)
	if strings.Contains(got, secret) {
		t.Fatalf("credential survived redaction: %q", got)
	}
	if strings.ContainsAny(got, "\r\n\x1b") {
		t.Fatalf("control character survived sanitization: %q", got)
	}
	if len([]rune(got)) > MaxDiagnosticRunes+1 {
		t.Fatalf("diagnostic has %d runes", len([]rune(got)))
	}
}
