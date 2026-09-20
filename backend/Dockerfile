# Multi-stage build for the TaskForge backend.
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/taskforged ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /out/taskforged /app/taskforged
EXPOSE 8080
ENTRYPOINT ["/app/taskforged"]
