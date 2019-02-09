package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const extensionID = 0
const hostnameID = 0

// Parses a Vector object (length prefixed bytes) per the TLS spec
func parseVector(buf []byte, lenBytes int) ([]byte, []byte, error) {
	if len(buf) < lenBytes {
		return nil, nil, errors.New("Not enough space in packet for vector")
	}
	var l int
	for _, b := range buf[:lenBytes] {
		l = (l << 8) + int(b)
	}
	if len(buf) < l+lenBytes {
		return nil, nil, errors.New("Not enough space in packet for vector")
	}
	return buf[lenBytes : l+lenBytes], buf[l+lenBytes:], nil
}

func readSNI(buf []byte) (string, error) {
	if len(buf) == 0 {
		return "", errors.New("Zero length handshake record")
	}
	if buf[0] != 1 {
		return "", fmt.Errorf("Non-ClientHello handshake record type %d", buf[0])
	}

	buf, _, err := parseVector(buf[1:], 3)
	if err != nil {
		return "", fmt.Errorf("Reading ClientHello: %s", err)
	}

	if len(buf) < 34 {
		return "", errors.New("ClientHello packet too short")
	}

	if buf[0] != 3 || buf[1] < 1 || buf[1] > 3 {
		return "", fmt.Errorf("ClientHello has unsupported version %d.%d", buf[0], buf[1])
	}

	// Skip version and random struct
	buf = buf[34:]

	vec, buf, err := parseVector(buf, 1)
	if err != nil {
		return "", fmt.Errorf("Reading ClientHello SessionID: %s", err)
	}
	if len(vec) > 32 {
		return "", fmt.Errorf("ClientHello SessionID too long (%db)", len(vec))
	}

	vec, buf, err = parseVector(buf, 2)
	if err != nil {
		return "", fmt.Errorf("Reading ClientHello CipherSuites: %s", err)
	}
	if len(vec) < 2 || len(vec)%2 != 0 {
		return "", fmt.Errorf("ClientHello CipherSuites invalid length %d", len(vec))
	}

	vec, buf, err = parseVector(buf, 1)
	if err != nil {
		return "", fmt.Errorf("Reading ClientHello CompressionMethods: %s", err)
	}
	if len(vec) < 1 {
		return "", fmt.Errorf("ClientHello CompressionMethods invalid length %d", len(vec))
	}

	if len(buf) != 0 {
		// Check vector is proper length for remaining msg
		vec, buf, err = parseVector(buf, 2)
		if err != nil {
			return "", fmt.Errorf("Reading ClientHello extensions: %s", err)
		}
		if len(buf) != 0 {
			return "", fmt.Errorf("%d bytes of trailing garbage in ClientHello", len(buf))
		}
		buf = vec

		for len(buf) >= 4 {
			typ := binary.BigEndian.Uint16(buf[:2])
			vec, buf, err = parseVector(buf[2:], 2)
			if err != nil {
				return "", fmt.Errorf("Reading ClientHello extension %d: %s", typ, err)
			}
			if typ == extensionID {
				// We found an SNI extension, attempt to extract the hostname
				buf, _, err := parseVector(vec, 2)
				if err != nil {
					return "", err
				}

				for len(buf) >= 3 {
					typ := buf[0]
					vec, buf, err = parseVector(buf[1:], 2)
					if err != nil {
						return "", errors.New("Truncated SNI extension")
					}

					// This vec is a hostname, return
					if typ == hostnameID {
						return string(vec), nil
					}
				}

				if len(buf) != 0 {
					return "", errors.New("Trailing garbage at end of SNI extension")
				}

				return "", nil
			}
		}
	}
	return "", errors.New("No SNI found")
}

func readVerAndSNI(r io.Reader) (string, int, error) {
	var header struct {
		Type         uint8
		Major, Minor uint8
		Length       uint16
	}
	if err := binary.Read(r, binary.BigEndian, &header); err != nil {
		return "", 0, fmt.Errorf("Error reading TLS record header: %s", err)
	}

	if header.Type != 22 {
		return "", 0, fmt.Errorf("TLS record is not a handshake")
	}

	if header.Major != 3 || header.Minor < 1 || header.Minor > 3 {
		return "", 0, fmt.Errorf("TLS record has unsupported version %d.%d",
			header.Major, header.Minor)
	}

	if header.Length > 16384 {
		return "", 0, errors.New("TLS record is malformed")
	}

	buf := make([]byte, header.Length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", 0, err
	}

	sni, err := readSNI(buf)
	return sni, int(header.Minor), err
}
