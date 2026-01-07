package middleware

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"

	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	AUTH_TYPE_KEY    contextKey = "auth_type"
	AUTH_SUBJECT_KEY contextKey = "auth_subject"
	JWT_CLAIMS_KEY   contextKey = "jwt_claims"
)

// AuthConfig holds authentication configuration
type AuthConfig struct {
	JWTPublicKey string // RSA public key in PEM format
	APIKeys      []string
}

// AuthResult holds the result of authentication
type AuthResult struct {
	Success     bool
	AuthType    string // "jwt" or "apikey"
	Claims      *jwt.RegisteredClaims
	AuthSubject string
	Error       error
}

// SplitAuthHeader splits an Authorization header into type and credentials
func SplitAuthHeader(authHeader string) []string {
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 {
		return []string{}
	}
	parts[0] = strings.ToLower(parts[0])
	return parts
}

// Authenticate validates the Authorization header and returns the authentication result
// This is a reusable function that can be called from middleware or GraphQL handlers
func Authenticate(authHeader string, cfg AuthConfig) AuthResult {
	// Create a map for faster API key lookup
	apiKeyMap := make(map[string]bool)
	for _, key := range cfg.APIKeys {
		if key != "" {
			apiKeyMap[key] = true
		}
	}

	result := AuthResult{
		Success: false,
	}

	if authHeader == "" {
		result.Error = errors.New("missing Authorization header")
		return result
	}

	// Parse the authorization header
	parts := SplitAuthHeader(authHeader)
	if len(parts) != 2 {
		result.Error = errors.New("invalid Authorization header format")
		return result
	}

	authType := parts[0]
	credentials := parts[1]

	switch authType {
	case "bearer":
		// JWT authentication
		claims, err := validateJWT(credentials, cfg.JWTPublicKey)
		if err != nil {
			result.Error = err
			return result
		}
		result.Success = true
		result.AuthType = "jwt"
		result.Claims = claims
		if claims.Subject != "" {
			result.AuthSubject = claims.Subject
		}

	case "apikey":
		// API Key authentication
		err := validateAPIKey(credentials, apiKeyMap)
		if err != nil {
			result.Error = err
			return result
		}
		result.Success = true
		result.AuthType = "apikey"

	default:
		result.Error = fmt.Errorf("unsupported authorization type: %s", authType)
		return result
	}

	return result
}

// Auth returns a gin middleware for authentication
// It supports both JWT (Bearer token) and API Key authentication
func Auth(cfg AuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		result := Authenticate(authHeader, cfg)

		if !result.Success {
			logger.WarnCtx(c.Request.Context(), "Authentication failed",
				zap.Error(result.Error),
				zap.String("path", c.Request.URL.Path),
				zap.String("client_ip", c.ClientIP()),
			)
			apiErr := apierrors.NewUnauthorizedError("Authentication failed", result.Error.Error())
			c.AbortWithStatusJSON(http.StatusUnauthorized, apiErr)
			return
		}

		// Store authentication info in context
		c.Set(AUTH_TYPE_KEY, result.AuthType)
		if result.Claims != nil {
			c.Set(JWT_CLAIMS_KEY, result.Claims)
			logger.DebugCtx(c.Request.Context(), "JWT authentication successful",
				zap.String("path", c.Request.URL.Path),
				zap.String("client_ip", c.ClientIP()),
				zap.String("subject", result.Claims.Subject),
			)
		} else {
			logger.DebugCtx(c.Request.Context(), "API Key authentication successful",
				zap.String("path", c.Request.URL.Path),
				zap.String("client_ip", c.ClientIP()),
			)
		}
		if result.AuthSubject != "" {
			c.Set(AUTH_SUBJECT_KEY, result.AuthSubject)
		}

		c.Next()
	}
}

// APIKeyAuth returns a gin middleware for API key authentication only
// It only accepts API Key authentication, JWT tokens will be rejected
func APIKeyAuth(cfg AuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")

		// Use the shared Authenticate function
		result := Authenticate(authHeader, cfg)

		// Check if authentication failed
		if !result.Success {
			logger.WarnCtx(c.Request.Context(), "Authentication failed",
				zap.Error(result.Error),
				zap.String("path", c.Request.URL.Path),
				zap.String("client_ip", c.ClientIP()),
			)
			apiErr := apierrors.NewUnauthorizedError("Authentication failed", result.Error.Error())
			c.AbortWithStatusJSON(http.StatusUnauthorized, apiErr)
			return
		}

		// Enforce API key only - reject JWT tokens
		if result.AuthType != "apikey" {
			logger.WarnCtx(c.Request.Context(), "Authentication failed: only API key authentication is allowed",
				zap.String("path", c.Request.URL.Path),
				zap.String("client_ip", c.ClientIP()),
				zap.String("auth_type", result.AuthType),
			)
			apiErr := apierrors.NewUnauthorizedError("Authentication failed", "only API key authentication is allowed for this endpoint")
			c.AbortWithStatusJSON(http.StatusUnauthorized, apiErr)
			return
		}

		// Store authentication info in context
		c.Set(AUTH_TYPE_KEY, result.AuthType)
		logger.DebugCtx(c.Request.Context(), "API Key authentication successful",
			zap.String("path", c.Request.URL.Path),
			zap.String("client_ip", c.ClientIP()),
		)

		c.Next()
	}
}

// validateJWT validates a JWT token with RSA signature and returns claims
func validateJWT(tokenString string, publicKeyPEM string) (*jwt.RegisteredClaims, error) {
	if publicKeyPEM == "" {
		return nil, errors.New("jwt public key not configured")
	}

	// Parse the RSA public key
	publicKey, err := parseRSAPublicKey(publicKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to parse RSA public key: %w", err)
	}

	// Parse and validate the token with claims
	claims := &jwt.RegisteredClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		// Validate the signing method is RSA
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return publicKey, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, errors.New("invalid token")
	}

	// Validate standard claims
	now := time.Now()

	// Check expiration
	if claims.ExpiresAt != nil && claims.ExpiresAt.Before(now) {
		return nil, errors.New("token has expired")
	}

	// Check not before
	if claims.NotBefore != nil && claims.NotBefore.After(now) {
		return nil, errors.New("token not yet valid")
	}

	return claims, nil
}

// parseRSAPublicKey parses an RSA public key from PEM format
func parseRSAPublicKey(publicKeyPEM string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(publicKeyPEM))
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing public key")
	}

	// Try parsing as PKIX (most common format)
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		// Try parsing as PKCS1 format
		return x509.ParsePKCS1PublicKey(block.Bytes)
	}

	rsaKey, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("public key is not an RSA key")
	}

	return rsaKey, nil
}

// validateAPIKey validates an API key
func validateAPIKey(apiKey string, validKeys map[string]bool) error {
	if len(validKeys) == 0 {
		return errors.New("no API keys configured")
	}

	if !validKeys[apiKey] {
		return errors.New("invalid API key")
	}

	return nil
}
