package utils

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"io"
)

func EncodeBase64(seq []byte) string {
	var buffer bytes.Buffer
	wc := base64.NewEncoder(base64.StdEncoding, &buffer)
	wc.Write(seq)
	wc.Close()

	return buffer.String()
}

func DecodeBase64(seq []byte) ([]byte, error) {
	rc := base64.NewDecoder(base64.StdEncoding, bytes.NewReader(seq))
	decoded, err := io.ReadAll(rc)

	if err != nil {
		return nil, err
	}

	return decoded, nil
}

func RandomToken(length int) (string, error) {
	randomBytesBuffer := make([]byte, length)
	if _, err := rand.Read(randomBytesBuffer); err != nil {
		return "", err
	}

	return EncodeBase64(randomBytesBuffer), nil
}
