package middleware

import (
	"strings"

	"cdc-cms-service/config"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

func JWTAuth(cfg *config.AppConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return c.Status(401).JSON(fiber.Map{"error": "missing authorization header"})
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")

		token, err := jwt.Parse(tokenString, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fiber.NewError(401, "unexpected signing method")
			}
			return []byte(cfg.JWT.Secret), nil
		})

		if err != nil || !token.Valid {
			return c.Status(401).JSON(fiber.Map{"error": "invalid token"})
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			return c.Status(401).JSON(fiber.Map{"error": "invalid claims"})
		}

		c.Locals("username", claims["username"])
		c.Locals("role", claims["role"])
		// Forward-compat: if the IdP ever starts emitting `roles: []string`
		// we expose it alongside the legacy single-role field. Both shapes
		// are consulted by internal/middleware/rbac.go.
		if rs, ok := claims["roles"]; ok {
			c.Locals("roles", rs)
		}
		return c.Next()
	}
}

func GetUsername(c *fiber.Ctx) string {
	if u, ok := c.Locals("username").(string); ok {
		return u
	}
	return "system"
}

func GetRole(c *fiber.Ctx) string {
	if r, ok := c.Locals("role").(string); ok {
		return r
	}
	return ""
}

// RequireRole returns middleware that enforces one of the allowed roles.
// Usage: router.Use(middleware.RequireRole("admin", "operator"))
func RequireRole(roles ...string) fiber.Handler {
	allowed := make(map[string]bool, len(roles))
	for _, r := range roles {
		allowed[r] = true
	}
	return func(c *fiber.Ctx) error {
		role := GetRole(c)
		if !allowed[role] {
			return c.Status(403).JSON(fiber.Map{
				"error": "forbidden: requires role " + strings.Join(roles, " or "),
			})
		}
		return c.Next()
	}
}
