package auth

// The manager has already checked the requested upstream model before invoking
// a built-in selector with an empty model to retain its shared rotation cursor.
// Hide only the aggregate model-unavailable flag in that selector's local view.
// Auth-wide cooldowns and the shared runtime lifecycle remain authoritative.
func preparedAuthsForEmptyModelSelection(auths []*Auth) []*Auth {
	var prepared []*Auth
	for index, auth := range auths {
		if auth == nil || !auth.Unavailable || auth.CooldownScope == cooldownScopeAuth || len(auth.ModelStates) == 0 {
			continue
		}
		if prepared == nil {
			prepared = append([]*Auth(nil), auths...)
		}
		view := *auth
		view.Unavailable = false
		prepared[index] = &view
	}
	if prepared == nil {
		return auths
	}
	return prepared
}
