package types

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
