.PHONY: run install

install:
	uv pip install streamlit pandas plotly openpyxl rapidfuzz

run:
	.venv/bin/streamlit run sheet.py
