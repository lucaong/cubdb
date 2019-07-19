VERSION = $(shell mix run -e "CubDB.Mixfile.project() |> Keyword.get(:version) |> IO.write()")
VERSION_TAG = v$(VERSION)

.PHONY: test
test:
	mix format --check-formatted lib/**/*.ex
	mix dialyzer --halt-exit-status
	mix coveralls --include property_based

.PHONY: benchmarks
benchmarks:
	@echo "\nBenchmark: put/3"
	@echo "================"
	mix run benchmarks/put.exs
	@echo "\nBenchmark: get/3"
	@echo "================"
	mix run benchmarks/get.exs

.PHONY: release
release: test
	@if [ $(shell git rev-parse --abbrev-ref HEAD) != master ]; then \
		echo "Error: not on master branch"; exit 1; \
	else true; fi
	@if [ $(git rev-parse $(VERSION_TAG)) ]; then \
		echo "Error: tag $(VERSION_TAG) already exists"; exit 1; \
	else true; fi
	@if [ -n "$(shell git status --porcelain)" ]; then \
		echo "Error: there are uncommitted changes"; exit 1; \
	else true; fi
	git tag $(VERSION_TAG)
	git push
	git push --tags
	open https://github.com/lucaong/cubdb/releases/new?tag=$(VERSION_TAG)
