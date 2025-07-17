.PHONY: .uv
.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: setup
setup: .uv
	uv sync

.PHONY: run
run: .uv
	uv run aemet.py

.PHONY: api-specs
api-specs:
	@curl -s -o aemet-api-specs.json https://opendata.aemet.es/AEMET_OpenData_specification.json

.PHONY: clean
clean:
	rm -rf data/
