/* =====================================================================
 *  Функция:  pg2links.find_anomalous_user_access
 * ---------------------------------------------------------------------
 *  Идея
 *  ----
 *  Для каждого пользователя ищем «похожих» (совпадает не менее
 *  p_min_match_count из 5-ти атрибутов: department, position, role,
 *  programming_language, agilestruct).  По ним считаем, как часто
 *  встречается каждый access.  Если доступ реже порога p_min_support
 *  и при этом у пользователя достаточно «похожих» (≥ p_min_similar),
 *  связь помечаем аномальной.
 *
 *  Параметры
 *  ---------
 *    p_min_support      NUMERIC   — минимальная доля (0–1), ниже которой
 *                                   доступ считается редким внутри группы
 *    p_min_similar      INTEGER   — минимум «похожих» для статистики
 *    p_min_match_count  INTEGER   — сколько атрибутов из 5-ти должно
 *                                   совпасть, чтобы юзера считать похожим
 *
 *  Возвращает
 *  ----------
 *    user_id            INTEGER   — пользователь
 *    access_id          INTEGER   — подозрительный доступ
 *    cluster_size       INTEGER   — сколько «похожих» найдено
 *    cluster_access_cnt INTEGER   — сколько из них имеют этот доступ
 *    support_ratio      NUMERIC   — доля: cluster_access_cnt/cluster_size
 *    anomaly_score      NUMERIC   — 1 - support_ratio (чем ближе к 1,
 *                                   тем «страннее» связь)
 * ===================================================================== */
CREATE OR REPLACE FUNCTION pg2links.find_anomalous_user_access(
        p_min_support      NUMERIC  DEFAULT 0.10,
        p_min_similar      INTEGER  DEFAULT 3,
        p_min_match_count  INTEGER  DEFAULT 3)
RETURNS TABLE (
        user_id             INTEGER,
        access_id           INTEGER,
        cluster_size        INTEGER,
        cluster_access_cnt  INTEGER,
        support_ratio       NUMERIC,
        anomaly_score       NUMERIC
) LANGUAGE plpgsql AS
$$
BEGIN
    /*---------------------------------------------------------------
     *  Блок CTE-запросов; результат последнего SELECT возвращается
     *  как выходная таблица функции (RETURN QUERY).
     *---------------------------------------------------------------*/
    RETURN QUERY
    WITH
    /*----------------------------------------------------------------
     * 1. sim_pairs  — все пары (user_id, sim_user_id), где sim_user_id
     *    «похож» на user_id по ≥ p_min_match_count атрибутам.
     *    (IS NOT DISTINCT FROM = «равно или оба NULL»).
     *    Каждое совпадение переводим в 1/0 при помощи ::int и суммируем.
     *----------------------------------------------------------------*/
    sim_pairs AS (
        SELECT u1.id AS user_id,
               u2.id AS sim_user_id
        FROM   pg2links.users u1
        JOIN   pg2links.users u2
               ON ( (u1.department           IS NOT DISTINCT FROM u2.department)::int +
                    (u1.position             IS NOT DISTINCT FROM u2.position)::int +
                    (u1.role                 IS NOT DISTINCT FROM u2.role)::int +
                    (u1.programming_language IS NOT DISTINCT FROM u2.programming_language)::int +
                    (u1.agilestruct          IS NOT DISTINCT FROM u2.agilestruct)::int
                  ) >= p_min_match_count
    ),

    /*----------------------------------------------------------------
     * 2. cluster_size — сколько «похожих» у каждого user_id
     *----------------------------------------------------------------*/
    cluster_size AS (
        SELECT sp.user_id,
               COUNT(*)::integer AS cluster_size   -- COUNT(*) → bigint, приводим к int
        FROM   sim_pairs sp
        GROUP  BY sp.user_id
    ),

    /*----------------------------------------------------------------
     * 3. cluster_access_cnt — для user_id считаем, сколько из его
     *    «похожих» обладают тем или иным access_id
     *----------------------------------------------------------------*/
    cluster_access_cnt AS (
        SELECT sp.user_id,
               ua.access_id,
               COUNT(*)::integer AS cluster_access_cnt
        FROM   sim_pairs sp
        JOIN   pg2links.user_access ua
               ON ua.user_id = sp.sim_user_id
        GROUP  BY sp.user_id, ua.access_id
    ),

    /*----------------------------------------------------------------
     * 4. scored — объединяем реальные связи пользователя с
     *    посчитанной статистикой и рассчитываем support_ratio
     *----------------------------------------------------------------*/
    scored AS (
        SELECT  ua.user_id,
                ua.access_id,
                cs.cluster_size,
                COALESCE(cac.cluster_access_cnt, 0)       AS cluster_access_cnt,
                /* доля = сколько-похожих-с-доступом / сколько-похожих-всего */
                COALESCE(cac.cluster_access_cnt, 0)::NUMERIC
                    / cs.cluster_size::NUMERIC            AS support_ratio
        FROM   pg2links.user_access ua
        JOIN   cluster_size          cs  ON cs.user_id = ua.user_id
        LEFT   JOIN cluster_access_cnt cac
               ON  cac.user_id  = ua.user_id
               AND cac.access_id = ua.access_id
    )

    /*----------------------------------------------------------------
     * 5. Финальный SELECT: фильтруем редкие связи и возвращаем
     *----------------------------------------------------------------*/
    SELECT  s.user_id,
            s.access_id,
            s.cluster_size,
            s.cluster_access_cnt,
            s.support_ratio,
            1 - s.support_ratio                           AS anomaly_score
    FROM    scored s
    WHERE   s.cluster_size  >= p_min_similar     -- игнорируем «шумные» кластеры
      AND   s.support_ratio <  p_min_support     -- редкость доступа ниже порога
    ORDER BY anomaly_score DESC,                 -- сначала самые странные
             s.user_id,
             s.access_id;
END;
$$;



\set min_common_attrs 4          -- порог, суммируются ВЕСА (а не просто число атрибутов)

BEGIN;                           -- ▀▀▀▀▀  единая транзакция  ▀▀▀▀▀

------------------------------------------------------------------
-- Шаг W. Таблица весов атрибутов ― редактируйте по необходимости
------------------------------------------------------------------
CREATE TEMP TABLE tmp_attr_weight (
    attr_name text PRIMARY KEY,
    weight    int  NOT NULL
) ON COMMIT DROP;

INSERT INTO tmp_attr_weight(attr_name, weight) VALUES
    ('department'          , 2),
    ('position'            , 1),
    ('role'                , 1),
    ('programming_language', 1),
    ('agilestruct'         , 1);



------------------------------------------------------------------
-- STEP 0. “UNPIVOT” users  →  (user_id, attr_name, attr_value)
------------------------------------------------------------------
CREATE TEMP TABLE tmp_user_attr ON COMMIT DROP AS
SELECT  u.id AS user_id,
        a.attr_name,
        COALESCE(a.attr_value::text, '__NULL__') AS attr_value
FROM    pg2links.users u
CROSS   JOIN LATERAL (
        VALUES
            ('department'          , u.department),
            ('position'            , u."position"),
            ('role'                , u.role),
            ('programming_language', u.programming_language),
            ('agilestruct'         , u.agilestruct)
        ) AS a(attr_name, attr_value);

CREATE INDEX tmp_user_attr_idx
        ON tmp_user_attr (attr_name, attr_value, user_id);

ANALYZE tmp_user_attr;



------------------------------------------------------------------
-- STEP 1. Пары пользователей, совпавшие хотя бы по ОДНОМУ атрибуту,
--         плюс ВЕС этого атрибута
------------------------------------------------------------------
CREATE TEMP TABLE tmp_pairs_raw ON COMMIT DROP AS
SELECT  ua1.user_id,
        ua2.user_id     AS sim_user_id,
        w.weight
FROM    tmp_user_attr        ua1
JOIN    tmp_user_attr        ua2  ON  ua1.attr_name  = ua2.attr_name
                                 AND ua1.attr_value = ua2.attr_value
                                 AND ua1.user_id    < ua2.user_id      -- избегаем дубляжа
JOIN    tmp_attr_weight       w    ON w.attr_name    = ua1.attr_name;

CREATE INDEX tmp_pairs_raw_idx
        ON tmp_pairs_raw (user_id, sim_user_id);

ANALYZE tmp_pairs_raw;



------------------------------------------------------------------
-- STEP 2. Суммируем веса по паре (user_id, sim_user_id)
------------------------------------------------------------------
CREATE TEMP TABLE tmp_sim_pairs ON COMMIT DROP AS
SELECT  user_id,
        sim_user_id,
        SUM(weight) AS score
FROM    tmp_pairs_raw
GROUP  BY user_id, sim_user_id
HAVING  SUM(weight) >= :'min_common_attrs';

CREATE INDEX tmp_sim_pairs_idx
        ON tmp_sim_pairs (user_id);

ANALYZE tmp_sim_pairs;



------------------------------------------------------------------
-- STEP 3. Размер «кластера» для каждого user_id
------------------------------------------------------------------
CREATE TEMP TABLE tmp_cluster_size ON COMMIT DROP AS
SELECT  user_id,
        COUNT(*) AS cluster_size
FROM (
    SELECT user_id,     sim_user_id FROM tmp_sim_pairs
    UNION ALL
    SELECT sim_user_id, user_id     FROM tmp_sim_pairs
) AS all_dir(user_id, sim_user_id)
GROUP BY user_id;

CREATE INDEX tmp_cluster_size_idx
        ON tmp_cluster_size (user_id);

ANALYZE tmp_cluster_size;



------------------------------------------------------------------
-- STEP 4. Сколько раз access_id встречается у «похожих»
------------------------------------------------------------------
CREATE TEMP TABLE tmp_cluster_access_cnt ON COMMIT DROP AS
SELECT  sp.user_id,
        ua.access_id,
        COUNT(*) AS cluster_access_cnt
FROM    tmp_sim_pairs        sp
JOIN    pg2links.user_access ua  ON ua.user_id = sp.sim_user_id
GROUP  BY sp.user_id, ua.access_id;

CREATE INDEX tmp_cluster_access_cnt_idx
        ON tmp_cluster_access_cnt (user_id, access_id);

ANALYZE tmp_cluster_access_cnt;



------------------------------------------------------------------
-- STEP 5. Итоговая метрика аномалии
------------------------------------------------------------------
SELECT  ua.user_id,
        ua.access_id,
        cs.cluster_size,
        COALESCE(cac.cluster_access_cnt, 0)                            AS cluster_access_cnt,
        COALESCE(cac.cluster_access_cnt, 0)::numeric / cs.cluster_size AS support_ratio,
        1 - COALESCE(cac.cluster_access_cnt, 0)::numeric / cs.cluster_size
                                                                       AS anomaly_score
FROM    pg2links.user_access  ua
JOIN    tmp_cluster_size      cs   ON cs.user_id = ua.user_id
LEFT JOIN tmp_cluster_access_cnt cac
           ON cac.user_id  = ua.user_id
          AND cac.access_id = ua.access_id
ORDER  BY anomaly_score DESC;     -- пример сортировки

COMMIT;                           -- ▀▀▀▀▀  конец единой транзакции  ▀▀▀▀▀



------------------------------
--- с полными совпадениями
------------------------------
---------------------------------------------------------------
-- 1. Пользователь  →  кластер
--    NULL-ы считаем «значимыми», поэтому подменяем маркером
---------------------------------------------------------------
CREATE TEMP TABLE tmp_user_clusters ON COMMIT DROP AS
SELECT  u.id                                             AS user_id,
        md5(
            coalesce(u.department,'__NULL__')           ||'|'||
            coalesce(u.position,'__NULL__')             ||'|'||
            coalesce(u.role,'__NULL__')                 ||'|'||
            coalesce(u.programming_language,'__NULL__') ||'|'||
            coalesce(u.agilestruct,'__NULL__')
        )                                               AS cluster_key
FROM    pg2links.users u;

CREATE INDEX tmp_user_clusters_cluster_key_idx
        ON tmp_user_clusters (cluster_key);


---------------------------------------------------------------
-- 2. Размер кластера
---------------------------------------------------------------
CREATE TEMP TABLE tmp_cluster_size ON COMMIT DROP AS
SELECT  cluster_key,
        COUNT(*)                AS cluster_size
FROM    tmp_user_clusters
GROUP BY cluster_key;

CREATE INDEX tmp_cluster_size_pk
        ON tmp_cluster_size (cluster_key);


---------------------------------------------------------------
-- 3. Сколько раз access_id встречается в кластере
---------------------------------------------------------------
CREATE TEMP TABLE tmp_cluster_access_cnt ON COMMIT DROP AS
SELECT  uc.cluster_key,
        ua.access_id,
        COUNT(*)                AS cluster_access_cnt
FROM    tmp_user_clusters  uc
JOIN    pg2links.user_access ua      ON ua.user_id = uc.user_id
GROUP BY uc.cluster_key, ua.access_id;

CREATE INDEX tmp_cluster_access_cnt_pk
        ON tmp_cluster_access_cnt (cluster_key, access_id);


---------------------------------------------------------------
-- 4. Финальный расчёт метрик
---------------------------------------------------------------
SELECT  uc.user_id,
        ua.access_id,
        cs.cluster_size,
        COALESCE(cac.cluster_access_cnt, 0)                              AS cluster_access_cnt,
        COALESCE(cac.cluster_access_cnt, 0)::numeric / cs.cluster_size   AS support_ratio,
        1 - COALESCE(cac.cluster_access_cnt, 0)::numeric / cs.cluster_size
                                                                        AS anomaly_score
FROM    tmp_user_clusters          uc
JOIN    pg2links.user_access       ua  ON ua.user_id = uc.user_id
JOIN    tmp_cluster_size           cs  ON cs.cluster_key = uc.cluster_key
LEFT JOIN tmp_cluster_access_cnt   cac
           ON cac.cluster_key = uc.cluster_key
          AND cac.access_id  = ua.access_id;


