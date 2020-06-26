#!/bin/bash

protoc -I . netkv.proto --go_out=plugins=grpc:.
