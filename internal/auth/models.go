// Package auth provides authentication functionality for various AI service providers.
// It includes interfaces and implementations for token storage and authentication methods.
package auth

// TokenStorage defines the interface for storing authentication tokens.
// Implementations of this interface should provide methods to persist
// authentication tokens to a file system location.
type TokenStorage interface {
	// SaveTokenToFile persists authentication tokens to the specified file path.
	//
	// Parameters:
	//   - authFilePath: The file path where the authentication tokens should be saved
	//
	// Returns:
	//   - error: An error if the save operation fails, nil otherwise
	SaveTokenToFile(authFilePath string) error
}

// TokenDataMarshaler is an optional interface implemented by token storages
// that can produce their persisted payload without writing to a path.
type TokenDataMarshaler interface {
	MarshalTokenData() ([]byte, error)
}

// MetadataSetter is an optional interface for token storages that can accept
// metadata before persisting their payload.
type MetadataSetter interface {
	SetMetadata(map[string]any)
}

// MetadataSnapshotter exposes the current injected metadata so a failed
// persistence transaction can restore token storage state.
type MetadataSnapshotter interface {
	MetadataSnapshot() map[string]any
}
