phony: readme lint

readme: #generates the README.md file
	@goreadme -functions -badge-godoc -badge-github=ci.yaml -badge-goreportcard -credit=false -skip-sub-packages > README.md

lint: #runs the go linter
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
	@golangci-lint run