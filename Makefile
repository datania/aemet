.PHONY: .uv
.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: setup
setup: .uv
	uv sync

.PHONY: estaciones
estaciones:
	./aemet estaciones -o data

.PHONY: valores-climatologicos
valores-climatologicos:
	./aemet valores-climatologicos --start 1920-01-01 --end 2025-01-01 -o data
