package version

import "fmt"

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// String returns human-readable version information.
func String() string {
	return fmt.Sprintf("ssecat %s (commit=%s date=%s)", Version, Commit, Date)
}
