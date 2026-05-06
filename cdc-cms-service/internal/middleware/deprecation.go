// Package middleware — deprecation.go (Phase 4 D2 — API prefix unify)
//
// Backwards-compat shim. The pre-D2 router exposed admin endpoints under
// a mix of /api/* and /api/v1/*; D2 ruling unifies on /api/v1/* canonical
// while keeping /api/* alive until clients migrate.
//
// Usage:
//   1. SetupRoutes mounts each non-/v1 route under both prefixes via
//      the dualX helpers in router.go.
//   2. Audit + Idempotency middlewares call CanonicalAPIRoute(route)
//      to fold the /v1 alias into the legacy key namespace, so the
//      action map and Redis cache stay single-namespace.
//   3. DeprecateLegacyAPIPath stamps RFC 8594 Sunset / Deprecation
//      headers + a successor-version Link onto responses where the
//      request hit the legacy alias, so clients self-discover the
//      canonical path.
package middleware

import (
	"strings"

	"github.com/gofiber/fiber/v2"
)

// CanonicalAPIRoute folds the /api/v1 alias into the legacy /api form so
// audit ActionMap + idempotency cache key stay single-namespace across
// the migration. Non-/api routes pass through unchanged.
func CanonicalAPIRoute(route string) string {
	if strings.HasPrefix(route, "/api/v1/") {
		return "/api" + strings.TrimPrefix(route, "/api/v1")
	}
	return route
}

// DeprecateLegacyAPIPath returns a Fiber middleware that adds RFC 8594
// Sunset / Deprecation headers + a successor-version Link to responses
// where the request hit /api/* but NOT /api/v1/*. Callers should set
// sunsetHTTPDate to an HTTP-date string (RFC 1123) marking when the
// legacy alias goes away; pass empty to omit the Sunset header.
func DeprecateLegacyAPIPath(sunsetHTTPDate string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		path := c.Path()
		if strings.HasPrefix(path, "/api/") && !strings.HasPrefix(path, "/api/v1/") {
			canonical := "/api/v1" + strings.TrimPrefix(path, "/api")
			c.Set("Deprecation", "true")
			if sunsetHTTPDate != "" {
				c.Set("Sunset", sunsetHTTPDate)
			}
			c.Set("Link", `<`+canonical+`>; rel="successor-version"`)
		}
		return c.Next()
	}
}
