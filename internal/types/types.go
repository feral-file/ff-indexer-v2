package types

import "regexp"

// StringPtr converts a string to a pointer to a string
func StringPtr(s string) *string {
	return &s
}

// StringNilOrEmpty checks if a pointer to a string is nil or empty
func StringNilOrEmpty(s *string) bool {
	return s == nil || *s == ""
}

// SafeString returns a safe string from a pointer to a string
func SafeString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// IsPositiveNumeric checks if a string is a valid positive numeric value
func IsPositiveNumeric(s string) bool {
	regex := regexp.MustCompile(`^[1-9][0-9]*$`)
	return regex.MatchString(s)
}
