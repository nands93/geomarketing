SHELL := /bin/bash
.PHONY: run clean install

run: install
	@echo "Executando programa..."
	@source venv/bin/activate && python3 geomarketing.py

install:
	@echo "Instalando dependências..."
	@python3 -m venv venv
	@source venv/bin/activate && pip install --upgrade pip
	@source venv/bin/activate && pip install -r requirements.txt

clean:
	@echo "Limpando arquivos gerados..."
	@rm -f *.html *.png dados_processados/*.png dados_processados/*.parquet