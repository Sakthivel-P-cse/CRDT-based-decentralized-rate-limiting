FROM golang:1.26-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git protobuf

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /rate-limiter ./cmd/node

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /rate-limiter .

EXPOSE 50051

ENTRYPOINT ["./rate-limiter"]