package server

import "github.com/valkey/valkey/internal/config"

// Authenticator defines the interface for connection authentication.
// The ACL-based implementation replaces the old single-password check.
type Authenticator interface {
	// Required returns true if clients must authenticate before running commands.
	Required() bool

	// Authenticate checks credentials. Returns the ACLUser on success, nil on failure.
	// If username is empty, the "default" user is used.
	Authenticate(username, password string) *ACLUser

	// CheckCommand verifies command+key permissions for a user.
	CheckCommand(user *ACLUser, cmd string, keys []string) error

	// ACL returns the underlying ACL for management commands.
	ACL() *ACL
}

// aclAuth implements Authenticator using the full ACL system.
type aclAuth struct {
	acl *ACL
}

// NewAuthenticator creates an ACL-based Authenticator from the security config.
func NewAuthenticator(cfg *config.SecurityConfig) Authenticator {
	return &aclAuth{acl: NewACL(cfg)}
}

func (a *aclAuth) Required() bool {
	return a.acl.Required()
}

func (a *aclAuth) Authenticate(username, password string) *ACLUser {
	return a.acl.Authenticate(username, password)
}

func (a *aclAuth) CheckCommand(user *ACLUser, cmd string, keys []string) error {
	return a.acl.CheckCommand(user, cmd, keys)
}

func (a *aclAuth) ACL() *ACL {
	return a.acl
}
