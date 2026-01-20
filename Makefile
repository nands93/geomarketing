SHELL := /bin/bash
.PHONY: run clean

run:
	@echo "Executando programa..."
	@source venv/bin/activate && python3 geomarketing.py

clean:
	@echo "Limpando arquivos gerados..."
	@rm -f *.html *.png dados_processados/*.png dados_processados/*.parquet