package crawler

import (
	"crypto/sha1"
	"fmt"
)

func SHA1Hash(token []byte) string {
	hash := sha1.New()
	hash.Write(token)
	return fmt.Sprintf("%x", hash.Sum(nil))
}
