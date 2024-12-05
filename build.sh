#! /bin/bash

go build -o bin/master ./src/master
go build -o bin/server ./src/server
go build -o bin/client ./src/client
go build -o bin/eaasclient ./src/eaasclient
