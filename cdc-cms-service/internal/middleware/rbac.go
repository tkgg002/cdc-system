// Package middleware — rbac.go (Phase 4 Security)
//
// Role-based access control for destructive admin endpoints.
//
// The upstream JWT middleware (jwt.go) already extracts two signals:
//   - c.Locals("role")      → single-role string claim (legacy shape)
//   - c.Locals("username")  → subject identifier
//
// We augment this with support for:
//   - multi-role claim `roles: []string`  (forward compatible)
//   - env-var fallback `ADMIN_USERS=user1,user2`
//     → everyone in this list is treated as having role "ops-admin",
//       which unblocks phase-4 rollout on deployments where the IdP
//       hasn't yet been updated to issue `roles` claims. Document this
//       as a TODO in the workspace implementation doc — it must be
//       replaced by real RBAC before prod.
//
// Usage:
//
//	admin := r.Group("", middleware.RequireOpsAdmin())
//	admin.Post("/api/recon/heal/:table", handler.TriggerHeal)
package middleware

import (
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// Canonical role name used by Phase-4 destructive endpoints.
const RoleOpsAdmin = "ops-admin"

// adminFallbackUsers reads ADMIN_USERS once at first use. It is purely
// a bridge while the IdP is being updated to emit `roles` claims. We
// deliberately do NOT cache globally — the env is read fresh so tests
// can mutate it with t.Setenv without restarting.
func adminFallbackUsers() map[string]struct{} {
	raw := os.Getenv("ADMIN_USERS")
	if raw == "" {
		return nil
	}
	out := make(map[string]struct{})
	for _, u := range strings.Split(raw, ",") {
		u = strings.TrimSpace(u)
		if u != "" {
			out[u] = struct{}{}
		}
	}
	return out
}

// userHasRole checks whether the JWT-derived claims on c satisfy any
// of `wanted`. It accepts both the legacy `role: string` shape and the
// newer `roles: []string` shape, plus the ADMIN_USERS env fallback for
// the ops-admin role.
func userHasRole(c *fiber.Ctx, wanted ...string) bool {
	want := make(map[string]struct{}, len(wanted))
	for _, r := range wanted {
		want[r] = struct{}{}
	}

	// Shape 1: single-role string claim.
	if r, ok := c.Locals("role").(string); ok && r != "" {
		if _, hit := want[r]; hit {
			return true
		}
	}

	// Shape 2: multi-role claim.
	if rs, ok := c.Locals("roles").([]string); ok {
		for _, r := range rs {
			if _, hit := want[r]; hit {
				return true
			}
		}
	}
	// MapClaims decodes arrays as []interface{}, handle that too.
	if rs, ok := c.Locals("roles").([]interface{}); ok {
		for _, v := range rs {
			if s, ok := v.(string); ok {
				if _, hit := want[s]; hit {
					return true
				}
			}
		}
	}

	// Shape 3: ADMIN_USERS env fallback (grants ops-admin only).
	if _, wantOps := want[RoleOpsAdmin]; wantOps {
		if admins := adminFallbackUsers(); admins != nil {
			if u, ok := c.Locals("username").(string); ok && u != "" {
				if _, isAdmin := admins[u]; isAdmin {
					return true
				}
			}
		}
	}

	return false
}

// RequireAnyRole returns a middleware that allows the request through
// iff the authenticated user holds at least one of the listed roles.
// On denial it emits HTTP 403 with a structured body including the
// required roles so the FE can present a clear error.
func RequireAnyRole(roles ...string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Guard: if JWT middleware wasn't wired upstream, locals will
		// be empty. Return 401 to make the missing-auth path explicit
		// rather than falling through as "forbidden".
		if c.Locals("username") == nil && c.Locals("role") == nil && c.Locals("roles") == nil {
			return c.Status(401).JSON(fiber.Map{"error": "unauthenticated"})
		}

		if userHasRole(c, roles...) {
			return c.Next()
		}
		return c.Status(403).JSON(fiber.Map{
			"error":          "forbidden",
			"required_role":  strings.Join(roles, "|"),
			"required_roles": roles,
		})
	}
}

// RequireOpsAdmin is the Phase-4 shorthand for destructive endpoints.
//
// BACKWARD-COMPAT: we also accept the legacy "admin" role until every
// deployed JWT issuer rolls out the new "ops-admin" claim. This is a
// temporary widening — see TODO in 03_implementation_v3_security_phase4.md.
// To tighten once IdP is updated, drop "admin" from the list below.
func RequireOpsAdmin() fiber.Handler {
	return RequireAnyRole(RoleOpsAdmin, "admin")
}
