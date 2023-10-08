run: build
	./bin/pbsb.exe
build: proto go
clean: 
	rm -rf ./bin ./gen
go:
	go build -o ./bin/ ./cmd/...
proto:
	mkdir gen
	protoc -I=./proto --go_out=./gen ./proto/types.proto