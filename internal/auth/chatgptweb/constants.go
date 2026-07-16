package chatgptweb

import "time"

const (
	Provider = "chatgpt-web"

	OAuthClientID = "app_2SKx67EdpoN0G6j64rFvigXD"
	AuthBaseURL   = "https://auth.openai.com"
	RedirectURL   = "https://platform.openai.com/auth/callback"
	AudienceURL   = "https://api.openai.com/v1"

	DefaultAcquisitionTimeout = 30 * time.Second
	DefaultRefreshLead        = 5 * time.Minute

	auth0Client = "eyJuYW1lIjoiYXV0aDAtc3BhLWpzIiwidmVyc2lvbiI6IjEuMjEuMCJ9"
)

const (
	LifecycleLoginPending        LifecycleState = "login_pending"
	LifecycleActive              LifecycleState = "active"
	LifecycleRefreshing          LifecycleState = "refreshing"
	LifecycleReloginPending      LifecycleState = "relogin_pending"
	LifecycleReauthRequired      LifecycleState = "reauth_required"
	LifecycleInteractionRequired LifecycleState = "interaction_required"
	LifecycleDead                LifecycleState = "dead"
)
