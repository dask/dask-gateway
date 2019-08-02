package main

import (
	"io"
	"net/http"
	"os"
	"strings"
)

type RouteMsg struct {
	Target string `json:"target"`
}

func isAuthorized(r *http.Request, token string) bool {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return false
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || parts[0] != "token" || parts[1] != token {
		return false
	}
	return true
}

func withAuthorization(handler http.HandlerFunc, token string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !isAuthorized(r, token) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		handler(w, r)
	}
}

func serveAPI(handler http.HandlerFunc, address string, token string, logger *Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/routes/", withAuthorization(handler, token))

	server := &http.Server{
		Addr:    address,
		Handler: mux,
	}
	err := server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		logger.Errorf("%s", err)
		os.Exit(1)
	}
}

func awaitShutdown() {
	buf := make([]byte, 10)
	for {
		_, err := os.Stdin.Read(buf)
		if err == io.EOF {
			os.Exit(0)
		}
	}
}
