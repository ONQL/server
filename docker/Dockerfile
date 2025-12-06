# Build Stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies.
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN go build -o onql-server cmd/server/main.go

# Run Stage
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/onql-server .

# Expose port (default 5656)
ENV PORT=5656
EXPOSE 5656

# Persist data
VOLUME ["/app/store"]

# Command to run the executable
CMD ["./onql-server"]

