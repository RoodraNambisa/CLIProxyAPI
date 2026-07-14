package auth

import (
	"fmt"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

// ProjectSelectionError is retained for v6 source compatibility with the removed Gemini CLI login flow.
type ProjectSelectionError struct {
	Email    string
	Projects []interfaces.GCPProjectProjects
}

func (e *ProjectSelectionError) Error() string {
	if e == nil {
		return "cliproxy auth: project selection required"
	}
	return fmt.Sprintf("cliproxy auth: project selection required for %s", e.Email)
}

// ProjectsDisplay returns the legacy projects list.
func (e *ProjectSelectionError) ProjectsDisplay() []interfaces.GCPProjectProjects {
	if e == nil {
		return nil
	}
	return e.Projects
}

// EmailRequiredError indicates that the calling context must provide an email or alias.
type EmailRequiredError struct {
	Prompt string
}

func (e *EmailRequiredError) Error() string {
	if e == nil || e.Prompt == "" {
		return "cliproxy auth: email is required"
	}
	return e.Prompt
}
