FROM golang:1.25-alpine AS build
ENV GOWORK=off
WORKDIR /src
COPY go.mod go.sum ./
COPY adapters/redis/go.mod adapters/redis/go.sum adapters/redis/
COPY examples/gin/go.mod examples/gin/go.sum examples/gin/
RUN cd examples/gin && go mod download
COPY . .
WORKDIR /src/examples/gin
RUN CGO_ENABLED=0 go build -o /out/server .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /out/server /usr/local/bin/server
USER nobody
ENTRYPOINT ["/usr/local/bin/server"]
