FROM golang:1.27 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/ce ./cmd/ce

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ce /ce
USER nonroot:nonroot
ENTRYPOINT ["/ce"]
