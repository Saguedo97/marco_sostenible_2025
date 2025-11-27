# Marco Sostenible 2025 – Pipeline Reproducible (MEF + GGGI)

Este repositorio contiene el pipeline reproducible utilizado para:

1. Procesar las bases del MEF (Gasto 2022–2025 e inversiones).
2. Enriquecer proyectos con brechas, KPIs y geografía.
3. Aplicar fuzzy matching + embeddings usando el archivo de semillas del Marco 2025.
4. Clasificar proyectos según criterios elegibles alineados al Marco de Bonos Sostenibles e ICMA.
5. Exportar un pipeline de proyectos elegibles en formato Excel para el MEF y GGGI.

Todo el proceso es **auditable, reproducible y documentado**, cumpliendo con los requisitos del contrato de consultoría con GGGI.

---

## 1. Estructura del repositorio

```text
marco_sostenible_2025/
├── data/
│   ├── bronze/               # Datos crudos del MEF (no versionados)
│   │   ├── gasto/
│   │   ├── universo/
│   │   └── brechas/
│   ├── silver/               # Datos procesados intermedios (pipeline)
│   ├── seeds/                # Archivo de semillas del Marco 2025
│   └── gold/                 # Excels finales para MEF / GGGI
├── notebooks/                # Notebooks de validación y exploración
├── outputs/                  # Gráficos, tablas o reportes opcionales
├── src/
│   └── marco_2025/
│        ├── __init__.py
│        ├── paths.py
│        ├── gasto_pipeline.py
│        ├── nlp_marco2025.py
│        └── export_gold.py
├── config.yaml               # Configuración de rutas y parámetros NLP
├── requirements.txt          # Dependencias de Python
├── run_pipeline.py           # Script único para ejecutar el pipeline completo
└── README.md
