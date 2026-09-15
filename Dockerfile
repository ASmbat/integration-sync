FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /integration-sync ./cmd/server

FROM alpine:3.19
COPY --from=build /integration-sync /integration-sync
EXPOSE 8080
ENTRYPOINT ["/integration-sync"]
