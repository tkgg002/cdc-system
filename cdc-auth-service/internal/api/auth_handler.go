package api

import (
	"cdc-auth-service/internal/service"

	"github.com/gofiber/fiber/v2"
)

type AuthHandler struct {
	authSvc *service.AuthService
}

func NewAuthHandler(authSvc *service.AuthService) *AuthHandler {
	return &AuthHandler{authSvc: authSvc}
}

// Login godoc
// @Summary      User login
// @Description  Authenticate user and return JWT access + refresh tokens
// @Tags         Auth
// @Accept       json
// @Produce      json
// @Param        body body service.LoginRequest true "Login credentials"
// @Success      200 {object} service.TokenResponse
// @Failure      400 {object} map[string]string
// @Failure      401 {object} map[string]string
// @Router       /api/auth/login [post]
func (h *AuthHandler) Login(c *fiber.Ctx) error {
	var req service.LoginRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	if req.Username == "" || req.Password == "" {
		return c.Status(400).JSON(fiber.Map{"error": "username and password are required"})
	}

	tokens, err := h.authSvc.Login(c.Context(), req)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(tokens)
}

// Register godoc
// @Summary      Register a new user
// @Description  Create a new user account (admin only in production)
// @Tags         Auth
// @Accept       json
// @Produce      json
// @Param        body body service.RegisterRequest true "Registration details"
// @Success      201 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Router       /api/auth/register [post]
func (h *AuthHandler) Register(c *fiber.Ctx) error {
	var req service.RegisterRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	if req.Username == "" || req.Email == "" || req.Password == "" {
		return c.Status(400).JSON(fiber.Map{"error": "username, email and password are required"})
	}

	user, err := h.authSvc.Register(c.Context(), req)
	if err != nil {
		return c.Status(409).JSON(fiber.Map{"error": err.Error()})
	}

	return c.Status(201).JSON(fiber.Map{"message": "user registered", "user": user})
}

// RefreshToken godoc
// @Summary      Refresh access token
// @Description  Exchange a valid refresh token for new access + refresh tokens
// @Tags         Auth
// @Accept       json
// @Produce      json
// @Param        body body object true "Refresh token" SchemaExample({"refresh_token":"eyJhbGci..."})
// @Success      200 {object} service.TokenResponse
// @Failure      400 {object} map[string]string
// @Failure      401 {object} map[string]string
// @Router       /api/auth/refresh [post]
func (h *AuthHandler) RefreshToken(c *fiber.Ctx) error {
	var req struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	if req.RefreshToken == "" {
		return c.Status(400).JSON(fiber.Map{"error": "refresh_token is required"})
	}

	tokens, err := h.authSvc.RefreshToken(c.Context(), req.RefreshToken)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(tokens)
}

// Health godoc
// @Summary      Health check
// @Tags         Health
// @Produce      json
// @Success      200 {object} map[string]string
// @Router       /health [get]
func (h *AuthHandler) Health(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok", "service": "cdc-auth"})
}
