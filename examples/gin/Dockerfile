FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
WORKDIR /src/examples/gin
RUN CGO_ENABLED=0 go build -o /out/server .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /out/server /usr/local/bin/server
ENTRYPOINT ["/usr/local/bin/server"]
