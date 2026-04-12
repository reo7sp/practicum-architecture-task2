package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Port         string
	KafkaBrokers string
}

type Producer struct {
	movieWriter   *kafka.Writer
	userWriter    *kafka.Writer
	paymentWriter *kafka.Writer
}

func NewProducer(brokers string) *Producer {
	return &Producer{
		movieWriter: &kafka.Writer{
			Addr:  kafka.TCP(brokers),
			Topic: "movie-events",
		},
		userWriter: &kafka.Writer{
			Addr:  kafka.TCP(brokers),
			Topic: "user-events",
		},
		paymentWriter: &kafka.Writer{
			Addr:  kafka.TCP(brokers),
			Topic: "payment-events",
		},
	}
}

func (p *Producer) Close() {
	p.movieWriter.Close()
	p.userWriter.Close()
	p.paymentWriter.Close()
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

func (p *Producer) PublishMovieEvent(ctx context.Context, event MovieEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", event.MovieID)),
		Value: eventJSON,
	}

	return p.movieWriter.WriteMessages(ctx, msg)
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  *string   `json:"username,omitempty"`
	Email     *string   `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

func (p *Producer) PublishUserEvent(ctx context.Context, event UserEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", event.UserID)),
		Value: eventJSON,
	}

	return p.userWriter.WriteMessages(ctx, msg)
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType *string   `json:"method_type,omitempty"`
}

func (p *Producer) PublishPaymentEvent(ctx context.Context, event PaymentEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", event.PaymentID)),
		Value: eventJSON,
	}

	return p.paymentWriter.WriteMessages(ctx, msg)
}

type Consumer struct {
	brokers string
}

func NewConsumer(brokers string) *Consumer {
	return &Consumer{
		brokers: brokers,
	}
}

func (c *Consumer) ConsumeMovieEvents(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{c.brokers},
		Topic:    "movie-events",
		GroupID:  "events-service-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading movie event: %v", err)
				continue
			}

			var event MovieEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Error unmarshaling movie event: %v", err)
				continue
			}

			log.Printf("Consumed movie event: movie_id=%d, title=%s, action=%s", event.MovieID, event.Title, event.Action)
		}
	}
}

func (c *Consumer) ConsumeUserEvents(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{c.brokers},
		Topic:    "user-events",
		GroupID:  "events-service-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading user event: %v", err)
				continue
			}

			var event UserEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Error unmarshaling user event: %v", err)
				continue
			}

			log.Printf("Consumed user event: user_id=%d, action=%s, timestamp=%s", event.UserID, event.Action, event.Timestamp.Format(time.RFC3339))
		}
	}
}

func (c *Consumer) ConsumePaymentEvents(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{c.brokers},
		Topic:    "payment-events",
		GroupID:  "events-service-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading payment event: %v", err)
				continue
			}

			var event PaymentEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Error unmarshaling payment event: %v", err)
				continue
			}

			log.Printf("Consumed payment event: payment_id=%d, user_id=%d, amount=%.2f, status=%s", event.PaymentID, event.UserID, event.Amount, event.Status)
		}
	}
}

func (c *Consumer) Start(ctx context.Context) {
	go c.ConsumeMovieEvents(ctx)
	go c.ConsumeUserEvents(ctx)
	go c.ConsumePaymentEvents(ctx)
}

type Handler struct {
	producer *Producer
}

func NewHandler(producer *Producer) *Handler {
	return &Handler{
		producer: producer,
	}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

func (h *Handler) HandleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	var event MovieEvent
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("Error decoding movie event: %v, body: %s", err, string(body))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if event.MovieID == 0 || event.Title == "" || event.Action == "" {
		http.Error(w, "movie_id, title, and action are required", http.StatusBadRequest)
		return
	}

	if err := h.producer.PublishMovieEvent(r.Context(), event); err != nil {
		log.Printf("Error publishing movie event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	eventJSON, _ := json.Marshal(event)
	response := EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    0,
		Event: Event{
			ID:        "movie-event",
			Type:      "movie",
			Timestamp: time.Now(),
			Payload:   eventJSON,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (h *Handler) HandleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	var event UserEvent
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("Error decoding user event: %v, body: %s", err, string(body))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if event.UserID == 0 || event.Action == "" || event.Timestamp.IsZero() {
		http.Error(w, "user_id, action, and timestamp are required", http.StatusBadRequest)
		return
	}

	if err := h.producer.PublishUserEvent(r.Context(), event); err != nil {
		log.Printf("Error publishing user event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	eventJSON, _ := json.Marshal(event)
	response := EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    0,
		Event: Event{
			ID:        "user-event",
			Type:      "user",
			Timestamp: time.Now(),
			Payload:   eventJSON,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func (h *Handler) HandlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	var event PaymentEvent
	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("Error decoding payment event: %v, body: %s", err, string(body))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if event.PaymentID == 0 || event.UserID == 0 || event.Amount == 0 || event.Status == "" || event.Timestamp.IsZero() {
		http.Error(w, "payment_id, user_id, amount, status, and timestamp are required", http.StatusBadRequest)
		return
	}

	if err := h.producer.PublishPaymentEvent(r.Context(), event); err != nil {
		log.Printf("Error publishing payment event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	eventJSON, _ := json.Marshal(event)
	response := EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    0,
		Event: Event{
			ID:        "payment-event",
			Type:      "payment",
			Timestamp: time.Now(),
			Payload:   eventJSON,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func main() {
	config := Config{
		Port:         getEnv("PORT", "8082"),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "kafka:9092"),
	}

	producer := NewProducer(config.KafkaBrokers)
	defer producer.Close()

	consumer := NewConsumer(config.KafkaBrokers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer.Start(ctx)

	handler := NewHandler(producer)

	http.HandleFunc("/health", handler.Health)
	http.HandleFunc("/api/events/health", handler.Health)
	http.HandleFunc("/api/events/movie", handler.HandleMovieEvent)
	http.HandleFunc("/api/events/user", handler.HandleUserEvent)
	http.HandleFunc("/api/events/payment", handler.HandlePaymentEvent)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Starting events service on port %s", config.Port)
		if err := http.ListenAndServe(":"+config.Port, nil); err != nil {
			log.Fatal(err)
		}
	}()

	<-sigChan
	log.Println("Shutting down...")
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
