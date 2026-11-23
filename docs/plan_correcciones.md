# Plan de correcciones: feedback de la 1ª entrega

**Resultado:** 10/10 (rúbrica 9,95). No hubo correcciones obligatorias.

| # | Observación del docente | Prioridad | Responsable | Fecha objetivo | Evidencia esperada | Estado |
|---|---|---|---|---|---|---|
| 1 | "Falta observar la defensa real" (Defensa 9/10) | Recomendada | Todo el grupo | 07/12/2026 | Defensa oral de la entrega final con participación de las 4 integrantes | Pendiente (entrega final): cada sección del notebook 02 tiene responsable |
| 2 | "No sobre-ingenierizar lo que ya está bien diseñado" | Obligatoria (criterio para E2) | Delfina (arquitectura) | 16/11/2026 | Alcance de E2 limitado al checklist §9.2 de la consigna; los extras van al backlog | Hecha: streaming hasta Bronze (D16), un mart Gold, 2 tablas Cassandra; el resto en `docs/BACKLOG.md` |
| 3 | Mantener la consistencia entre diagrama e implementación | Recomendada | Delfina | 16/11/2026 | Arquitectura v2 que refleje solo lo implementado | Hecha: `docs/img/arquitectura_v2.png` |

## Correcciones que surgieron al implementar

| # | Hallazgo | Corrección | Evidencia |
|---|---|---|---|
| 4 | El watermark de "2 days" del config de la E1 descartaba el 48 % de los eventos | Watermark de 60 días medido con un experimento (D15) | `evidence/02_watermark_experimento.csv` |
| 5 | `tags_json` de `resources` se leía truncado en la E1 | `escape='"'` + columna `_corrupt_record` (D21) | notebook 02 §2 |
| 6 | Esquemas duplicados entre el notebook 01 y `src/common/schemas.py` | El pipeline de E2 importa los esquemas y la config desde `src/` y `config/` | `src/jobs/*` |
| 7 | Carga a Cassandra prevista con el conector | Driver de Python (D18) | notebook 02 §8 |
| 8 | Lake en Drive (D14) | La demo corre en `/content` y se reconstruye en minutos | README Quickstart |

# Plan de correcciones: feedback de la 2ª entrega

**Resultado:** 10/10 (rúbrica 9,65).

| # | Observación del docente | Prioridad | Responsable | Evidencia | Estado |
|---|---|---|---|---|---|
| 9 | Cerrar la semántica de los costos negativos con una definición explícita (Procesamiento Spark 9,5) | Obligatoria | Abril (calidad) | D29 · notebook 03 §4.2.1 · `docs/DOCUMENTACION_TECNICA.md` §6 | Hecha: definición de negocio sustentada con 4 mediciones; los negativos con consumo se corrigen y quedan marcados, los 4 sin consumo van a quarantine |
| 10 | `cost_anomaly_mart` definitivo (Analítica 9,5) | Obligatoria | Abril / Agustina | `gold/cost_anomaly_mart` · D28 | Hecha: 190 días anómalos con costo anómalo, score máximo, comparación con la mediana y eventos corregidos |
| 11 | Mayor cobertura de pruebas de idempotencia, fallos y serving (Pruebas 9,0) | Recomendada | Delfina | `tests/test_idempotencia_fallos_serving.py` · `evidence/final_tests_pytest.txt` | Hecha: de 11 a 21 tests (pipeline de punta a punta dos veces, JSON corrupto, archivo faltante, conciliación fallida, carga y conteo en Cassandra) |
| 12 | Registro de fallos (observado al ampliar las pruebas) | Recomendada | Delfina | D30 · `RunLog.track` | Hecha: un job que falla queda con estado `ERROR` en `_meta/run_log` |
