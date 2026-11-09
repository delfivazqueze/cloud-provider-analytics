# Plan de correcciones: feedback de la 1ª entrega

**Resultado:** 10/10 (rúbrica 9,95). No hubo correcciones obligatorias.

| # | Observación del docente | Prioridad | Responsable | Fecha objetivo | Evidencia esperada | Estado |
|---|---|---|---|---|---|---|
| 1 | "Falta observar la defensa real" (Defensa 9/10) | Recomendada | Todo el grupo | 16/11/2026 | Las 4 integrantes explican su capa en la demo de la 2ª entrega | Planificada: cada sección del notebook 02 tiene responsable |
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
