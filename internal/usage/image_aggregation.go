package usage

// Generation attempts retain their tokens and diagnostics. Only delivery
// records contribute to the primary request and health denominators.
func (d RequestDetail) countsAsRequest() bool {
	return !d.Auxiliary && d.ImageAggregation.Role != "attempt"
}
