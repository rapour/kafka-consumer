.PHONY: protos

protos:
	protoc -I src/gateway --python_out=src/gateway/communication communication.proto