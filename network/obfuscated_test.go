package network_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/network"
)

// TestGenerateObfuscatedHeader_Validation verifies that the generated 64-byte
// obfuscated payload conforms exactly to MTProto DPI evasion rules.
func TestGenerateObfuscatedHeader_Validation(t *testing.T) {
	t.Parallel()

	targetDC := int16(2)

	// 1. Verify default direct connection (Abridged / ProtocolTypeEF -> 0xef)
	headerAbridged, err := network.GenerateObfuscatedHeader(targetDC, nil)
	require.NoError(t, err)
	assert.Len(t, headerAbridged, 64)
	assert.Equal(t, []byte{0xef, 0xef, 0xef, 0xef}, headerAbridged[56:60], "protocol signature must be 0xef for Abridged")
	dcIDAbridged := binary.BigEndian.Uint16(headerAbridged[60:62])
	assert.Equal(t, uint16(targetDC), dcIDAbridged, "target DC ID must be properly injected using big-endian format")

	// 2. Verify PaddedIntermediate explicit transport (ProtocolTypeDD -> 0xdd)
	headerPadded, err := network.GenerateObfuscatedHeaderForTransport(network.TransportPaddedIntermediate, targetDC, nil)
	require.NoError(t, err)
	assert.Len(t, headerPadded, 64)
	assert.Equal(t, []byte{0xdd, 0xdd, 0xdd, 0xdd}, headerPadded[56:60], "protocol signature must be 0xdd for PaddedIntermediate")

	// 3. Verify avoidance of forbidden DPI triggers
	val := binary.LittleEndian.Uint32(headerAbridged[0:4])
	val2 := binary.LittleEndian.Uint32(headerAbridged[4:8])

	assert.NotEqual(t, byte(0xef), headerAbridged[0])
	assert.NotEqual(t, uint32(0x00000000), val2)

	forbiddenVals := []uint32{
		0x44414548, // HEAD
		0x54534f50, // POST
		0x20544547, // GET
		0x4954504f, // OPTI
		0xeeeeeeee,
		0xdddddddd,
		0x02010316,
	}

	for _, fv := range forbiddenVals {
		assert.NotEqual(t, fv, val, "the header must not contain a forbidden DPI signature")
	}
}

// TestGenerateObfuscatedHeader_Retries uses a mock byte reader to guarantee that
// the function successfully bypasses and discards restricted byte sequences.
func TestGenerateObfuscatedHeader_Retries(t *testing.T) {
	t.Parallel()

	// 1. Construct a forbidden payload (e.g., matching "GET ")
	forbiddenPayload := make([]byte, 64)
	binary.LittleEndian.PutUint32(forbiddenPayload[0:4], 0x20544547)

	// 2. Construct a valid payload immediately following it
	validPayload := make([]byte, 64)
	validPayload[0] = 0xaa                                       // Valid starting byte
	binary.LittleEndian.PutUint32(validPayload[4:8], 0x11223344) // Valid val2

	// Provide a chained buffer consisting of the bad and good payloads
	mockReader := bytes.NewReader(append(forbiddenPayload, validPayload...))

	header, err := network.GenerateObfuscatedHeader(2, mockReader)
	require.NoError(t, err)

	// Assert that it skipped the first payload and securely picked the second one
	assert.Equal(t, byte(0xaa), header[0], "should have skipped the forbidden payload and used the valid one")
	assert.Equal(t, uint32(0x11223344), binary.LittleEndian.Uint32(header[4:8]))
}
