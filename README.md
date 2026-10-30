# data-feed

Histórico diario de ofertas de empleo y noticias Tech/Data/IA. Una tarea programada de Claude lee cada mañana los dos resúmenes que llegan a Gmail, los convierte en JSON y los guarda aquí.

```
ofertas/YYYY-MM.jsonl            una oferta por línea
noticias/YYYY-MM.jsonl           una noticia + borrador de post de LinkedIn por línea
seguimiento/candidaturas.jsonl   cambios de estado de las ofertas
schemas/                         campos de cada tipo de registro
ingest.py                        limpia URLs, valida, deduplica y escribe
```

## Uso

```bash
python ingest.py ofertas  inbox/ofertas.json
python ingest.py noticias inbox/noticias.json
python ingest.py estado linkedin:4484715007 aplicada --nota "CV v3"
```

Los registros no se editan nunca: las ofertas repetidas se descartan por `id` y el estado de una candidatura se añade como evento (`vista`, `interesante`, `aplicada`, `entrevista`, `oferta`, `rechazada`, `descartada`).

## Consultar

```sql
-- DuckDB: ofertas con su estado actual
WITH e AS (SELECT id, arg_max(estado, fecha) AS estado
           FROM 'seguimiento/candidaturas.jsonl' GROUP BY id)
SELECT o.fecha_email, o.puesto, o.empresa, o.ubicacion,
       coalesce(e.estado, 'nueva') AS estado, o.url
FROM 'ofertas/*.jsonl' o LEFT JOIN e USING (id)
ORDER BY o.fecha_email DESC;
```

Solo usa la librería estándar de Python.
