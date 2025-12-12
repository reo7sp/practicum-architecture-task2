package main

import (
	"encoding/binary"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
)

type Config struct {
	Port                   string
	MonolithURL            *url.URL
	MoviesServiceURL       *url.URL
	EventsServiceURL       *url.URL
	GradualMigration       bool
	MoviesMigrationPercent int
}

var config Config

func main() {
	monolithURL, err := url.Parse(getEnv("MONOLITH_URL", "http://monolith:8080"))
	if err != nil {
		log.Fatalf("Invalid monolith URL: %v", err)
	}

	moviesServiceURL, err := url.Parse(getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"))
	if err != nil {
		log.Fatalf("Invalid movies service URL: %v", err)
	}

	eventsServiceURL, err := url.Parse(getEnv("EVENTS_SERVICE_URL", "http://events-service:8082"))
	if err != nil {
		log.Fatalf("Invalid events service URL: %v", err)
	}

	config = Config{
		Port:                   getEnv("PORT", "8000"),
		MonolithURL:            monolithURL,
		MoviesServiceURL:       moviesServiceURL,
		EventsServiceURL:       eventsServiceURL,
		GradualMigration:       getEnv("GRADUAL_MIGRATION", "false") == "true",
		MoviesMigrationPercent: parseInt(getEnv("MOVIES_MIGRATION_PERCENT", "0")),
	}

	if config.MoviesMigrationPercent < 0 || config.MoviesMigrationPercent > 100 {
		log.Fatal("MOVIES_MIGRATION_PERCENT must be between 0 and 100")
	}

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/movies", handleMoviesProxy)
	http.HandleFunc("/api/movies/", handleMoviesProxy)
	http.HandleFunc("/api/events/", handleEventsProxy)
	http.HandleFunc("/", handleMonolithProxy)

	log.Fatal(http.ListenAndServe(":"+config.Port, nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Proxy is healthy"))
}

func handleMoviesProxy(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/movies/health" {
		proxyToMoviesService(w, r)
		return
	}

	if config.GradualMigration && shouldRouteToMoviesService() {
		proxyToMoviesService(w, r)
		return
	}

	proxyToMonolith(w, r)
}

func handleEventsProxy(w http.ResponseWriter, r *http.Request) {
	proxyToEventsService(w, r)
}

func handleMonolithProxy(w http.ResponseWriter, r *http.Request) {
	proxyToMonolith(w, r)
}

func shouldRouteToMoviesService() bool {
	randomValue := generateRandomPercent()
	return randomValue < config.MoviesMigrationPercent
}

func generateRandomPercent() int {
	var b [4]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return 0
	}
	value := binary.BigEndian.Uint32(b[:])
	return int(value % 100)
}

func proxyToMoviesService(w http.ResponseWriter, r *http.Request) {
	log.Printf("Proxying %s %s to movies-service", r.Method, r.URL.Path)

	proxy := httputil.NewSingleHostReverseProxy(config.MoviesServiceURL)
	proxy.ServeHTTP(w, r)
}

func proxyToEventsService(w http.ResponseWriter, r *http.Request) {
	log.Printf("Proxying %s %s to events-service", r.Method, r.URL.Path)

	proxy := httputil.NewSingleHostReverseProxy(config.EventsServiceURL)
	proxy.ServeHTTP(w, r)
}

func proxyToMonolith(w http.ResponseWriter, r *http.Request) {
	log.Printf("Proxying %s %s to monolith", r.Method, r.URL.Path)

	proxy := httputil.NewSingleHostReverseProxy(config.MonolithURL)
	proxy.ServeHTTP(w, r)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func parseInt(s string) int {
	value, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return value
}
