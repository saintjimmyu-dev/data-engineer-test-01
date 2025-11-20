
# Airbnb ETL Pipeline

Este proyecto implementa un pipeline ETL completo usando Python, SQLite y pruebas automatizadas con pytest.

## Estructura del proyecto
```
data-engineer-test-01/
├── README.md
├── SOLUTION.md
├── requirements.txt
├── .env.example
├── src/
│   ├── pipeline/
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── validate.py
│   │   ├── load.py
│   │   └── orchestrator.py
│   └── utils/
│       ├── db_connector.py
│       └── logger.py
├── data/
│   ├── listings.csv
│   └── reviews.csv
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_validate.py
│   ├── test_load.py
│   └── conftest.py (si existiera)
└── output/
    └── data_quality_report.json (se genera automáticamente)
```

## Ejecución del pipeline
```
python -m src.pipeline.orchestrator
```

## Ejecutar pruebas
```
pytest -v
```
