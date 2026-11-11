install-grpc:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

grpc/%.pb.go: grpc/%.proto
	protoc --go_out=. --go_opt=paths=source_relative grpc/$*.proto
grpc/%_grpc.pb.go: grpc/%.proto
	protoc --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/$*.proto

grpc: grpc/public/public.pb.go grpc/public/public_grpc.pb.go grpc/private/private.pb.go grpc/private/private_grpc.pb.go