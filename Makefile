VERSION = $(shell mix run -e "CubDB.Mixfile.project() |> Keyword.get(:version) |> IO.write()")
VERSION_TAG = v$(VERSION)

.PHONY: test
test:
	mix test --include property_based

.PHONY: benchmarks
benchmarks:
	mix run benchmarks/put.exs
	mix run benchmarks/get.exs

.PHONY: release
release: test
	@if [ $(shell git rev-parse --abbrev-ref HEAD) != master ]; then \
		echo "Error: not on master branch"; exit 1; \
	fi
	@if [ $(shell git rev-parse $(VERSION_TAG)) ]; then \
		echo "Error: tag $(VERSION_TAG) already exists"; exit 1; \
	fi
	@if [ -n "$(shell git status --porcelain)" ]; then \
		echo "Error: there are uncommitted changes"; exit 1; \
	fi
	git tag $(VERSION_TAG)
	git push
	git push --tags
	open https://github.com/lucaong/cubdb/releases/new?tag=$(VERSION_TAG)
