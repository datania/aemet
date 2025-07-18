.PHONY: .uv
.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: estaciones
estaciones: .uv
	uv run aemet estaciones -o dataset

.PHONY: valores-climatologicos
valores-climatologicos: .uv
	uv run aemet valores-climatologicos --start 1920-01-01 --end 2025-01-01 -o dataset

.PHONY: api-specs
api-specs:
	@curl -s -o aemet-api-specs.json https://opendata.aemet.es/AEMET_OpenData_specification.json

.PHONY: lint
lint:
	uvx ruff check
	uvx ty check

.PHONY: upload
upload:
	uvx --from "huggingface_hub[hf_xet]" huggingface-cli upload-large-folder \
		--token=${HUGGINGFACE_TOKEN} \
		--repo-type dataset \
		--num-workers 4 \
		datania/aemet dataset/

.PHONY: clean
clean:
	rm -rf dataset/
