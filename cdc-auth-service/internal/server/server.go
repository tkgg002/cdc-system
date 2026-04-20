package server

import (
	"fmt"

	"cdc-auth-service/config"
	"cdc-auth-service/internal/api"
	"cdc-auth-service/internal/repository"
	"cdc-auth-service/internal/service"
	"cdc-auth-service/pkgs/database"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	fiberlogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/swagger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Server struct {
	cfg    *config.AppConfig
	logger *zap.Logger
	db     *gorm.DB
	app    *fiber.App
}

func New(cfg *config.AppConfig, logger *zap.Logger) (*Server, error) {
	db, err := database.NewPostgresConnection(cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	logger.Info("PostgreSQL connected")

	// Repository
	userRepo := repository.NewUserRepo(db)

	// Service
	authSvc := service.NewAuthService(userRepo, cfg)

	// Handler
	authHandler := api.NewAuthHandler(authSvc)

	// Fiber app
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(fiberlogger.New())
	app.Use(cors.New())

	// Swagger UI
	app.Get("/swagger/*", swagger.HandlerDefault)

	// Public routes
	app.Get("/health", authHandler.Health)
	app.Post("/api/auth/login", authHandler.Login)
	app.Post("/api/auth/register", authHandler.Register)
	app.Post("/api/auth/refresh", authHandler.RefreshToken)

	return &Server{cfg: cfg, logger: logger, db: db, app: app}, nil
}

func (s *Server) Start() error {
	s.logger.Info("Auth Service started", zap.String("port", s.cfg.Server.Port))
	return s.app.Listen(s.cfg.Server.Port)
}

func (s *Server) Shutdown() {
	s.logger.Info("shutting down Auth Service...")
	s.app.Shutdown()
	sqlDB, _ := s.db.DB()
	sqlDB.Close()
	s.logger.Info("Auth Service stopped")
}
