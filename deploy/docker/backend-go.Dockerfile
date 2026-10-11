# Builds one backend-go binary, selected with --build-arg CMD=<name of cmd/ dir>.
# Build context: backend-go/
FROM golang:1.27-alpine AS build
ARG CMD
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN test -n "$CMD" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/app /app/app
COPY scenarios /app/scenarios
ENTRYPOINT ["/app/app"]
