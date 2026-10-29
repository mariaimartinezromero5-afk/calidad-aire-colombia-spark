# Análisis de la calidad del aire en Colombia con Apache Spark

Procesamiento en batch con PySpark del conjunto de datos "Calidad del Aire en
Colombia (Promedio Anual)", publicado por el IDEAM en datos.gov.co.

## Descripción
El script carga el CSV, limpia y transforma los datos y realiza un análisis
exploratorio (EDA): registros por contaminante, estaciones por año, promedio de
PM2.5 por autoridad ambiental, evolución anual de PM10 y PM2.5 y promedio por
contaminante. Los resultados se guardan como gráficos PNG.

## Fuente de datos
IDEAM, Subsistema de Información sobre Calidad del Aire (SISAIRE), datos.gov.co.
Descargar el CSV con Exportar -> CSV y guardarlo como `calidad_aire.csv`.

## Requisitos
- Python 3
- Apache Spark (PySpark)
- matplotlib y pandas

```bash
pip3 install matplotlib pandas
```

## Ejecución
```bash
spark-submit batch_calidad_aire.py calidad_aire.csv
```

## Resultados
Se generan tres gráficos:
- `grafico_pm25_autoridades.png`
- `grafico_evolucion_pm.png`
- `grafico_promedio_contaminante.png`

## Autor
María - Curso Big Data (202016911), UNAD
