package oneport

import "io"

// Any is a matcher that matches any connection
func Any() Matcher {
	return func(io.Reader) bool { return true }
}
