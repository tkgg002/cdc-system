package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cdc-auth-service/config"
	"cdc-auth-service/internal/model"
	"cdc-auth-service/internal/repository"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type AuthService struct {
	userRepo *repository.UserRepo
	cfg      *config.AppConfig
}

func NewAuthService(userRepo *repository.UserRepo, cfg *config.AppConfig) *AuthService {
	return &AuthService{userRepo: userRepo, cfg: cfg}
}

// LoginRequest is the request body for login
type LoginRequest struct {
	Username string `json:"username" example:"admin"`
	Password string `json:"password" example:"admin123"`
}

// RegisterRequest is the request body for registration
type RegisterRequest struct {
	Username string `json:"username" example:"operator1"`
	Email    string `json:"email" example:"op1@goopay.vn"`
	Password string `json:"password" example:"securepass"`
	FullName string `json:"full_name" example:"Nguyen Van A"`
	Role     string `json:"role" example:"operator"`
}

// TokenResponse is the response containing JWT tokens
type TokenResponse struct {
	AccessToken  string   `json:"access_token"`
	RefreshToken string   `json:"refresh_token"`
	ExpiresIn    int64    `json:"expires_in"` // seconds
	TokenType    string   `json:"token_type"`
	User         UserInfo `json:"user"`
}

// UserInfo is the user data returned in token response
type UserInfo struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
	Email    string `json:"email"`
	FullName string `json:"full_name"`
	Role     string `json:"role"`
}

func (s *AuthService) Login(ctx context.Context, req LoginRequest) (*TokenResponse, error) {
	user, err := s.userRepo.GetByUsername(ctx, req.Username)
	fmt.Println(user)
	if err != nil {
		return nil, errors.New("invalid username or password")
	}
	fmt.Println(user.Password)
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
		return nil, errors.New("invalid username or password")
	}

	return s.generateTokens(user)
}

func (s *AuthService) Register(ctx context.Context, req RegisterRequest) (*model.User, error) {
	exists, _ := s.userRepo.ExistsByUsername(ctx, req.Username)
	if exists {
		return nil, errors.New("username already taken")
	}

	existsEmail, _ := s.userRepo.ExistsByEmail(ctx, req.Email)
	if existsEmail {
		return nil, errors.New("email already registered")
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	role := req.Role
	if role == "" {
		role = "operator"
	}
	if role != "admin" && role != "operator" {
		return nil, errors.New("invalid role: must be admin or operator")
	}

	user := &model.User{
		Username: req.Username,
		Email:    req.Email,
		Password: string(hashedPassword),
		FullName: req.FullName,
		Role:     role,
		IsActive: true,
	}

	if err := s.userRepo.Create(ctx, user); err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}

	return user, nil
}

func (s *AuthService) RefreshToken(ctx context.Context, refreshTokenStr string) (*TokenResponse, error) {
	token, err := jwt.Parse(refreshTokenStr, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return []byte(s.cfg.JWT.Secret), nil
	})

	if err != nil || !token.Valid {
		return nil, errors.New("invalid refresh token")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid claims")
	}

	tokenType, _ := claims["type"].(string)
	if tokenType != "refresh" {
		return nil, errors.New("not a refresh token")
	}

	userIDFloat, _ := claims["user_id"].(float64)
	user, err := s.userRepo.GetByID(ctx, uint(userIDFloat))
	if err != nil {
		return nil, errors.New("user not found")
	}

	return s.generateTokens(user)
}

func (s *AuthService) generateTokens(user *model.User) (*TokenResponse, error) {
	now := time.Now()

	// Access token
	accessClaims := jwt.MapClaims{
		"user_id":  user.ID,
		"username": user.Username,
		"email":    user.Email,
		"role":     user.Role,
		"type":     "access",
		"iat":      now.Unix(),
		"exp":      now.Add(s.cfg.JWT.AccessExpiration).Unix(),
	}
	accessToken := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
	accessTokenStr, err := accessToken.SignedString([]byte(s.cfg.JWT.Secret))
	if err != nil {
		return nil, fmt.Errorf("sign access token: %w", err)
	}

	// Refresh token
	refreshClaims := jwt.MapClaims{
		"user_id": user.ID,
		"type":    "refresh",
		"iat":     now.Unix(),
		"exp":     now.Add(s.cfg.JWT.RefreshExpiration).Unix(),
	}
	refreshToken := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)
	refreshTokenStr, err := refreshToken.SignedString([]byte(s.cfg.JWT.Secret))
	if err != nil {
		return nil, fmt.Errorf("sign refresh token: %w", err)
	}

	return &TokenResponse{
		AccessToken:  accessTokenStr,
		RefreshToken: refreshTokenStr,
		ExpiresIn:    int64(s.cfg.JWT.AccessExpiration.Seconds()),
		TokenType:    "Bearer",
		User: UserInfo{
			ID:       user.ID,
			Username: user.Username,
			Email:    user.Email,
			FullName: user.FullName,
			Role:     user.Role,
		},
	}, nil
}
