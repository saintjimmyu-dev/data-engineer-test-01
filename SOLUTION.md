
# SOLUTION.md – Decisiones Técnicas

## 1. Diseño del Pipeline
El pipeline sigue un flujo clásico:
1. Extracción (extract.py)
2. Validación (validate.py)
3. Transformación (transform.py)
4. Carga en SQLite (load.py)
5. Orquestación (orchestrator.py)

## 2. Manejo de Calidad de Datos
Se generan:
- `status`: OK o WARNING
- `report`: diccionario con hallazgos

Esto permite continuar aunque existan advertencias.

## 3. Compatibilidad con Tests
Se ajustaron:
- Firmas de funciones
- Valores de retorno
- Validaciones tolerantes para datos pequeños
