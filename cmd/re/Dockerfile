FROM golang:1.27 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/re ./cmd/re

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/re /re
USER nonroot:nonroot
ENTRYPOINT ["/re"]
