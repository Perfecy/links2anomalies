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
