.PHONY: .uv
.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: estaciones
estaciones: .uv
	uv run aemet estaciones -o data

.PHONY: valores-climatologicos
valores-climatologicos: .uv
	uv run aemet valores-climatologicos --start 1920-01-01 --end 2025-01-01 -o data

.PHONY: api-specs
api-specs:
	@curl -s -o aemet-api-specs.json https://opendata.aemet.es/AEMET_OpenData_specification.json

.PHONY: lint
lint:
	uvx ruff check
	uvx ty check


.PHONY: clean
clean:
	rm -rf data/
