// Package network provides physical layer emulation boundaries and traffic tuning
// parameters to safely bypass DPI and mimic official Telegram clients.
package network

import (
	"crypto/rand"
	"encoding/binary"
	"io"
)

// GenerateObfuscatedHeaderForTransport creates the 64-byte initial payload required for MTProto
// Obfuscated2 protocol for a specified transport type (Abridged, Intermediate, or PaddedIntermediate).
// It safely avoids common HTTP method patterns to bypass DPI systems and injects the protocol signature
// and Target DC ID directly into the payload in accordance with official Telegram client standards.
func GenerateObfuscatedHeaderForTransport(transport TransportType, dcID int16, randSource io.Reader) ([64]byte, error) {
	if randSource == nil {
		randSource = rand.Reader
	}

	var b [64]byte

	for {
		if _, err := io.ReadFull(randSource, b[:]); err != nil {
			return b, err
		}

		val := binary.LittleEndian.Uint32(b[0:4])
		val2 := binary.LittleEndian.Uint32(b[4:8])

		// 1. Avoid Abridged protocol signature in the first byte.
		if b[0] == 0xef {
			continue
		}

		// 2. Avoid matching basic HTTP methods or core MTProto protocol signatures
		// encoded as little-endian uint32.
		// 0x44414548 = "HEAD", 0x54534f50 = "POST", 0x20544547 = "GET ", 0x4954504f = "OPTI"
		if val == 0x44414548 || val == 0x54534f50 || val == 0x20544547 || val == 0x4954504f ||
			val == 0xeeeeeeee || val == 0xdddddddd || val == 0x02010316 {
			continue
		}

		// 3. Ensure the second 4-byte block is not absolutely zero.
		if val2 == 0x00000000 {
			continue
		}

		// The random sequence is safe from DPI trigger rules.
		break
	}

	// Bytes 56-59: Protocol signature matching official Telegram clients.
	// Abridged (ProtocolTypeEF) uses 0xef; Padded Intermediate (ProtocolTypeDD) uses 0xdd.
	sigByte := byte(0xef)
	switch transport {
	case TransportPaddedIntermediate, TransportFakeTLS:
		sigByte = 0xdd
	case TransportIntermediate:
		sigByte = 0xee
	}

	b[56] = sigByte
	b[57] = sigByte
	b[58] = sigByte
	b[59] = sigByte

	// Bytes 60-61: Target Datacenter ID injected using Big-Endian format.
	binary.BigEndian.PutUint16(b[60:62], uint16(dcID))

	// Bytes 62-63 are left as securely generated random bytes.
	return b, nil
}

// GenerateObfuscatedHeader creates the 64-byte initial payload required for MTProto
// Obfuscated2 protocol with the default Abridged transport signature (0xef).
func GenerateObfuscatedHeader(dcID int16, randSource io.Reader) ([64]byte, error) {
	return GenerateObfuscatedHeaderForTransport(TransportAbridged, dcID, randSource)
}
