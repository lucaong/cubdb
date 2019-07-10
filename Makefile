.PHONY: test
test:
	mix test --include property_based

.PHONY: benchmarks
benchmarks:
	mix run benchmarks/put.exs
	mix run benchmarks/get.exs
