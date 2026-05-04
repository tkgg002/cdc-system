package admin

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Deps — dependency injection cho admin Server.
type Deps struct {
	DB                *gorm.DB
	NATS              *nats.Conn
	DebeziumBaseURL   string
	SchemaRegistryURL string
	AuthToken         string
	Logger            *zap.Logger
}

// Server — HTTP server cho cdc-admin-api.
type Server struct {
	deps   Deps
	engine *gin.Engine
}

// NewServer khởi tạo Server với Gin router + middleware.
// Nếu AuthToken rỗng → dev mode, log warning, skip auth.
func NewServer(deps Deps) *Server {
	if deps.AuthToken == "" {
		deps.Logger.Warn("ADMIN_API_TOKEN empty — auth disabled (dev mode only)")
	}
	s := &Server{deps: deps}
	s.engine = s.buildEngine()
	return s
}

func (s *Server) buildEngine() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(s.authMiddleware())
	r.GET("/healthz", func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) })
	r.POST("/v2/sources/register", s.handleRegisterSource)
	return r
}

// authMiddleware — Bearer token check. Skip /healthz. Skip nếu token empty (dev mode).
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/healthz" {
			c.Next()
			return
		}
		if s.deps.AuthToken == "" {
			// dev mode: không enforce auth
			c.Next()
			return
		}
		got := c.GetHeader("Authorization")
		want := "Bearer " + s.deps.AuthToken
		if got != want {
			c.AbortWithStatusJSON(401, gin.H{"error": "unauthorized"})
			return
		}
		c.Next()
	}
}

// Run bắt đầu HTTP server và graceful shutdown khi ctx cancel.
func (s *Server) Run(ctx context.Context, addr string) error {
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           s.engine,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()
	return httpSrv.ListenAndServe()
}
