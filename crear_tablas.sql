-- Table: public.unificada

-- DROP TABLE IF EXISTS public.unificada;

CREATE TABLE IF NOT EXISTS public.unificada
(
    level_0 bigint,
    level_1 bigint,
    cod_localidad bigint,
    id_provincia bigint,
    id_departamento bigint,
    "categoría" text COLLATE pg_catalog."default",
    provincia text COLLATE pg_catalog."default",
    localidad text COLLATE pg_catalog."default",
    nombre text COLLATE pg_catalog."default",
    domicilio text COLLATE pg_catalog."default",
    "código postal" text COLLATE pg_catalog."default",
    "número de teléfono" text COLLATE pg_catalog."default",
    mail text COLLATE pg_catalog."default",
    web text COLLATE pg_catalog."default",
    fuente text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.unificada
    OWNER to postgres;
-- Index: ix_unificada_level_0

-- DROP INDEX IF EXISTS public.ix_unificada_level_0;

CREATE INDEX IF NOT EXISTS ix_unificada_level_0
    ON public.unificada USING btree
    (level_0 ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_unificada_level_1

-- DROP INDEX IF EXISTS public.ix_unificada_level_1;

CREATE INDEX IF NOT EXISTS ix_unificada_level_1
    ON public.unificada USING btree
    (level_1 ASC NULLS LAST)
    TABLESPACE pg_default;

-- Table: public.registros_por_categoria

-- DROP TABLE IF EXISTS public.registros_por_categoria;

CREATE TABLE IF NOT EXISTS public.registros_por_categoria
(
    index bigint,
    "categoría" text COLLATE pg_catalog."default",
    cantidad bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.registros_por_categoria
    OWNER to postgres;
-- Index: ix_registros_por_categoria_index

-- DROP INDEX IF EXISTS public.ix_registros_por_categoria_index;

CREATE INDEX IF NOT EXISTS ix_registros_por_categoria_index
    ON public.registros_por_categoria USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;

-- Table: public.registros_por_fuente

-- DROP TABLE IF EXISTS public.registros_por_fuente;

CREATE TABLE IF NOT EXISTS public.registros_por_fuente
(
    index bigint,
    fuente text COLLATE pg_catalog."default",
    cantidad bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.registros_por_fuente
    OWNER to postgres;
-- Index: ix_registros_por_fuente_index

-- DROP INDEX IF EXISTS public.ix_registros_por_fuente_index;

CREATE INDEX IF NOT EXISTS ix_registros_por_fuente_index
    ON public.registros_por_fuente USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;


-- Table: public.registros_por_provincia_y_categoria

-- DROP TABLE IF EXISTS public.registros_por_provincia_y_categoria;

CREATE TABLE IF NOT EXISTS public.registros_por_provincia_y_categoria
(
    index bigint,
    provincia text COLLATE pg_catalog."default",
    "categoría" text COLLATE pg_catalog."default",
    cantidad bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.registros_por_provincia_y_categoria
    OWNER to postgres;
-- Index: ix_registros_por_provincia_y_categoria_index

-- DROP INDEX IF EXISTS public.ix_registros_por_provincia_y_categoria_index;

CREATE INDEX IF NOT EXISTS ix_registros_por_provincia_y_categoria_index
    ON public.registros_por_provincia_y_categoria USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;

-- Table: public.cines

-- DROP TABLE IF EXISTS public.cines;

CREATE TABLE IF NOT EXISTS public.cines
(
    index bigint,
    "Provincia" text COLLATE pg_catalog."default",
    "Pantallas" bigint,
    "Butacas" bigint,
    "Espacios INCAA" bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.cines
    OWNER to postgres;
-- Index: ix_cines_index

-- DROP INDEX IF EXISTS public.ix_cines_index;

CREATE INDEX IF NOT EXISTS ix_cines_index
    ON public.cines USING btree
    (index ASC NULLS LAST)
    TABLESPACE pg_default;

