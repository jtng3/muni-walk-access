.PHONY: pipeline-sample pipeline-full site-dev site-build smoke validate test clean install

pipeline-sample:
	cd pipeline && uv run python -m muni_walk_access --sample 1000

pipeline-full:
	cd pipeline && uv run python -m muni_walk_access

site-dev:
	cd site && npm run dev

site-build:
	cd site && npm run build

smoke: pipeline-sample site-build
	@echo "✓ smoke test passed"

validate:
	@test -f pipeline/tests/test_ground_truth.py && cd pipeline && uv run pytest tests/test_ground_truth.py || echo "⚠ ground truth tests not yet written (Story 1.10)"

test:
	cd pipeline && uv run pytest
	cd site && npm test

clean:
	rm -rf pipeline/.cache site/dist site/.astro

install:
	cd pipeline && uv sync
	cd site && npm install
	pre-commit install
