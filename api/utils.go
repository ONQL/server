package api

import "fmt"

// errorResponse creates a JSON error response
func errorResponse(msg string) string {
	return fmt.Sprintf(`{"error":"%s"}`, msg)
}
