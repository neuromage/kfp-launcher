FROM golang:alpine

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Move to working directory /build

# Copy and download dependency using go mod
# RUN go mod download

# Copy the code into the container
COPY cmd /build/cmd
COPY component /build/component
COPY metadata /build/metadata
COPY third_party /build/third_party
COPY go.mod /build/.
COPY go.sum /build/.

# Build the application
WORKDIR /build
RUN go build github.com/neuromage/kfp-launcher/cmd/launch

COPY launcher_container/mount_launcher.sh /bin/mount_launcher.sh
RUN chmod +x /bin/mount_launcher.sh

WORKDIR /bin
RUN cp /build/launch .

ENTRYPOINT ["/bin/launch", "--mlmd_server_address", "${METADATA_GRPC_SERVICE_HOST}", "--mlmd_server_port", "${METADATA_GRPC_SERVICE_PORT}"]