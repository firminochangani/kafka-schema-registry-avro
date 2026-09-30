include .env
export

.PHONY: install setup-kafka producer consumer

install:
	go install github.com/actgardner/gogen-avro/v10/cmd/...@latest

setup-kafka:
	./setup-kafka.sh

producer:
	reflex -s -r '\.go$$' go run ./ producer

consumer:
	reflex -s -r '\.go$$' go run ./ consumer

gen-schemas:
	gogen-avro --package=events ./events ./schemas/*