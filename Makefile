all:
	golangci-lint run
	CGO_ENABLED=0 go build -ldflags "-extldflags '-static'" 
	strip fido
