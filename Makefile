# Comenzi uzuale. Rust nu trebuie instalat local: cargo rulează în container.
CARGO := scripts/cargo.sh
TEX   := docker run --rm -v "$(CURDIR)":/w -w /w spd-texlive

.PHONY: help up down logs check fmt lint test consistency bench docs slides texlive clean

help:            ## Lista comenzilor
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

up:              ## Pornește tot stack-ul (build inclus)
	docker compose up -d --build

down:            ## Oprește stack-ul și șterge volumele
	docker compose down -v

logs:            ## Loguri aplicații
	docker compose logs -f app1 app2

fmt:             ## Formatează codul
	$(CARGO) fmt --all

lint:            ## Clippy (pedantic) cu warnings ca erori
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --workspace --all-targets -- -D warnings

test:            ## Teste unitare și de integrare
	$(CARGO) test --workspace

check: lint test ## fmt + clippy + teste

consistency:     ## Test end-to-end de coerență (stack pornit)
	scripts/consistency.sh

bench:           ## Toate experimentele -> results/*.csv (stack pornit)
	scripts/bench.sh
	python3 scripts/plots_data.py

texlive:
	@docker image inspect spd-texlive >/dev/null 2>&1 || docker build -t spd-texlive -f deploy/Dockerfile.docs deploy

docs: texlive    ## Documentația PDF -> docs/build/main.pdf
	python3 scripts/plots_data.py
	$(TEX) sh -c 'cd docs && latexmk -lualatex -interaction=nonstopmode -halt-on-error -outdir=build main.tex'

slides: texlive  ## Prezentarea PDF -> presentation/build/slides.pdf
	python3 scripts/plots_data.py
	$(TEX) sh -c 'cd presentation && latexmk -lualatex -interaction=nonstopmode -halt-on-error -outdir=build slides.tex'

clean:
	rm -rf docs/build presentation/build
