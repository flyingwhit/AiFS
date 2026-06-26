# Dockerfile

FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

ARG SERVICE
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/app ./cmd/${SERVICE}


FROM alpine:3.20

WORKDIR /app

COPY --from=builder /out/app /app/app

EXPOSE 8001 8002 8003 9000 9100 8080

ENTRYPOINT ["/app/app"]