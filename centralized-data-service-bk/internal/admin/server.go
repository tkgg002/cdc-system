package admin

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"gorm.io/gorm"
)

const (
	// 10 req/min per token (1 token every 6s) with burst capacity 3.
	adminRateInterval = 6 * time.Second
	adminRateBurst    = 3

	// maxRequestBodyBytes — 64 KiB hard cap on request body size.
	maxRequestBodyBytes = 64 * 1024
)

// rateLimiterStore — per-token token-bucket store. Single-instance only;
// memory-bounded by number of distinct tokens (typically <100).
type rateLimiterStore struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
}

func newRateLimiterStore() *rateLimiterStore {
	return &rateLimiterStore{limiters: make(map[string]*rate.Limiter)}
}

func (s *rateLimiterStore) get(key string) *rate.Limiter {
	s.mu.Lock()
	defer s.mu.Unlock()
	lim, ok := s.limiters[key]
	if !ok {
		lim = rate.NewLimiter(rate.Every(adminRateInterval), adminRateBurst)
		s.limiters[key] = lim
	}
	return lim
}

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
	deps    Deps
	engine  *gin.Engine
	rlStore *rateLimiterStore
}

// NewServer khởi tạo Server với Gin router + middleware.
// Nếu AuthToken rỗng → dev mode, log warning, skip auth.
func NewServer(deps Deps) *Server {
	if deps.AuthToken == "" {
		deps.Logger.Warn("ADMIN_API_TOKEN empty — auth disabled (dev mode only)")
	}
	s := &Server{deps: deps, rlStore: newRateLimiterStore()}
	s.engine = s.buildEngine()
	return s
}

func (s *Server) buildEngine() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(s.bodyLimitMiddleware())  // F1-5: first — cap body before any parsing
	r.Use(s.authMiddleware())       // F1-2: constant-time token check
	r.Use(s.rateLimitMiddleware())  // F1-3: per-token rate limit after auth
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
		// Length check first — ConstantTimeCompare requires equal length to be useful.
		// (Mismatched length = automatic reject; doesn't leak content timing.)
		if len(got) != len(want) ||
			subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			c.AbortWithStatusJSON(401, gin.H{"error": "unauthorized"})
			return
		}
		c.Next()
	}
}

// rateLimitMiddleware — per-token token-bucket. Skip /healthz + skip when
// auth is disabled (dev mode). Uses token from Authorization header as key.
func (s *Server) rateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/healthz" {
			c.Next()
			return
		}
		if s.deps.AuthToken == "" {
			c.Next()
			return
		}
		token := c.GetHeader("Authorization")
		if token == "" {
			c.Next() // auth middleware will reject
			return
		}
		lim := s.rlStore.get(token)
		if !lim.Allow() {
			c.Header("Retry-After", strconv.Itoa(int(adminRateInterval.Seconds())))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limited"})
			return
		}
		c.Next()
	}
}

// bodyLimitMiddleware — caps request body at 64 KiB (excluding /healthz which
// has no body). Trips MaxBytesReader on read → handler will see EOF or
// "request body too large" via gin's ShouldBindJSON.
func (s *Server) bodyLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/healthz" {
			c.Next()
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxRequestBodyBytes)
		c.Next()
	}
}

// Run bắt đầu HTTP server và graceful shutdown khi ctx cancel.
func (s *Server) Run(ctx context.Context, addr string) error {
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           s.engine,
		ReadHeaderTimeout: 10 * time.Second,
		MaxHeaderBytes:    maxRequestBodyBytes,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()
	return httpSrv.ListenAndServe()
}
