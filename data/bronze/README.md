# data/bronze

Esta carpeta NO contiene datos en GitHub.

Aquí van los **archivos crudos** del MEF que se usan como input del pipeline:

- `data/bronze/gasto/`
  - Archivos de gasto devengado 2022–2025  
    Ejemplos de nombres esperados:
    - `2022-Gasto.csv`
    - `2023-Gasto.csv`
    - `2024-Gasto-Diario.csv`
    - `2025-Gasto-Diario.csv`

- `data/bronze/universo/`
  - Archivos del universo de proyectos de inversión:
    - `DETALLE_INVERSIONES.csv`
    - `CIERRE_INVERSIONES.csv`
    - `INVERSIONES_DESACTIVADAS.csv`

- `data/bronze/brechas/`
  - Archivo de brechas de inversión:
    - `INVERSIONES_BRECHAS.csv`

Estos archivos **no se suben al repositorio** por:

1. **Tamaño** (varios superan los límites de GitHub).
2. **Sensibilidad** de la información.

Cada persona que use este repo debe obtener los datos desde la fuente correspondiente  
(p. ej. MEF / OneDrive interno) y colocarlos localmente respetando esta estructura de carpetas.
