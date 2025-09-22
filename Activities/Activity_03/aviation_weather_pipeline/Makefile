.PHONY: all fetch analyze clean

all: fetch analyze
fetch:
	@echo "Running fetch_metars.sh..."
	./fetch_metars.sh
analyze:
	@echo "Running analyze_data.sh..."
	./analyze_data.sh
clean:
	@echo "Cleaning up generated files..."
	rm -rf raw_metars weather_report.csv metar_summary.csv

