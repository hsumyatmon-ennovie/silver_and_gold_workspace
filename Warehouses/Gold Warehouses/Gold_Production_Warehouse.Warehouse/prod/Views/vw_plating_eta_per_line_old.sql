-- Auto Generated (Do not modify) F86E960A14F1618A106FCCFA5C296C7554E50194E09DEE6449C6AC27C87D96FE
CREATE   VIEW prod.vw_plating_eta_per_line_old
AS
/* =====================================================================
   PURPOSE
     Predict when each production order line will reach the PLATING step.
     Returns per-line ETA, remaining days, and simple status, picking one
     representative item per (order,line).
 
   FIXES (Synapse-friendly)
     - Replaced FOR XML string-agg with STRING_AGG in an OUTER APPLY.
     - Robust current-step matching:
         (A) numeric op_no → SeqNo (with light normalization),
         (B) exact name/code fallback,
         (C) partial name fallback (lowest priority).
     - Broader PLATING detection (group='PLATING' OR abbrev='PLT' OR name LIKE '%PLAT%').
     - Keep rows even when PLATING isn’t found (LEFT JOIN + null-safe math).
   ===================================================================== */
 
---------------------------------------------------------------------------
-- 0) ORDER/LINE/ITEM + TWO START DATES
---------------------------------------------------------------------------
WITH line0 AS (
    SELECT
        pol.prod_order_no,
        pol.prod_order_line_no,
        CONVERT(date, pol.prod_line_start_date) AS StartDatePlan, -- date only
        pol.prod_item_line,
        pol.prod_order_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY pol.prod_order_no, pol.prod_order_line_no, pol.prod_item_line
            ORDER BY pol.prod_line_start_date DESC
        ) AS rn
    FROM Gold_Production_Warehouse.prod.gold_production_order AS pol
    WHERE pol.prod_order_status = 'Released'
),
start_from_status AS (
    -- Actual start = earliest operation_no row per (order,line) from SILVER status.
    SELECT
        s.prod_order_no,
        s.prod_order_line_no,
        CONVERT(date, s.created_on) AS StartDateActual
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.prod_order_no, s.prod_order_line_no
                ORDER BY s.operation_no ASC
            ) AS rn
        FROM Silver_Production_Warehouse.prod.silver_production_status AS s
        WHERE s.operation_no IS NOT NULL AND LTRIM(RTRIM(s.operation_no)) <> ''
    ) AS s
    WHERE s.rn = 1
),
line AS (
    -- One row per (order,line,item), with both planned/actual starts.
    SELECT
        l0.prod_order_no,
        l0.prod_order_line_no,
        l0.prod_order_quantity,
        l0.prod_item_line,
        l0.StartDatePlan,
        sfs.StartDateActual
    FROM line0 AS l0
    LEFT JOIN start_from_status AS sfs
      ON sfs.prod_order_no      = l0.prod_order_no
     AND sfs.prod_order_line_no = l0.prod_order_line_no
    WHERE l0.rn = 1
),
 
---------------------------------------------------------------------------
-- 1) ROUTING & MAPPING
---------------------------------------------------------------------------
routing_raw AS (
    SELECT
        prl.prod_order_no,
        prl.prod_order_lineno,
        prl.item_no,
        TRY_CONVERT(decimal(18,3), prl.operation_no) AS SeqNo,  -- '010.01' → 10.01
        COALESCE(NULLIF(LTRIM(RTRIM(mc.current_operation)), ''), prl.routing_no) AS OpName,
        prl.routing_no                       AS RoutingCode,
        UPPER(prl.type_name)                 AS TypeNameU
    FROM Silver_Production_Warehouse.prod.silver_routing_lines AS prl
    LEFT JOIN Silver_Commons_Warehouse.cmn.silver_prod_step_casting_production AS mc
      ON mc.current_operation = prl.routing_no
    INNER JOIN line AS l
      ON prl.prod_order_no     = l.prod_order_no
     AND prl.prod_order_lineno = l.prod_order_line_no
    WHERE UPPER(prl.type_name) = 'MACHINE CENTER'
),
routing_norm AS (
    SELECT r.*,
           UPPER(LTRIM(RTRIM(r.OpName))) AS OpNameU
    FROM routing_raw AS r
),
op_map AS (
    SELECT
        UPPER(LTRIM(RTRIM(current_operation)))     AS MapKey,
        UPPER(LTRIM(RTRIM(operation_description))) AS DescKey,
        UPPER(LTRIM(RTRIM(operation_group)))       AS OpGroup,
        UPPER(LTRIM(RTRIM(operation_abb)))         AS OpAbb,
        CAST([Pro Due Date Offset_Day_] AS decimal(10,2)) AS OffsetDays
    FROM Silver_Commons_Warehouse.cmn.silver_prod_step_casting_production
),
routing_map AS (
    -- Map routing row → OpGroup/OpAbb/BaseDays (exact MapKey first; DescKey fallback)
    SELECT
        rn.*,
        m.OpGroup    AS MappedGroup,
        m.OffsetDays AS BaseDays,
        m.OpAbb      AS MappedAbbrev
    FROM routing_norm AS rn
    OUTER APPLY (
        SELECT TOP (1)
            om.OpGroup, om.OffsetDays, om.OpAbb
        FROM op_map AS om
        WHERE rn.OpNameU = om.MapKey
           OR rn.OpName  = om.MapKey
           OR om.DescKey LIKE '%' + rn.OpNameU + '%'
        ORDER BY CASE WHEN rn.OpNameU = om.MapKey THEN 0 ELSE 1 END,
                 om.OpAbb, om.OpGroup
    ) AS m
),
 
---------------------------------------------------------------------------
-- 2) DURATIONS (DAYS) + CUMULATIVE
---------------------------------------------------------------------------
routing_dur AS (
    SELECT
        rm.*,
        CAST(ROUND(
            COALESCE(
                CASE UPPER(NULLIF(rm.MappedAbbrev, ''))
                    WHEN 'FIL'   THEN 2.0
                    WHEN 'HT'    THEN 1.0
                    WHEN 'TUM'   THEN 1.0
                    WHEN 'LAS'   THEN 1.0
                    WHEN 'SET'   THEN 1.0
                    WHEN 'POL'   THEN 2.0
                    WHEN 'SHI'   THEN 1.0
                    WHEN 'PLT'   THEN 1.0
                    WHEN 'GLU'   THEN 2.0
                    WHEN 'QC'    THEN 1.0
                    WHEN 'QA'    THEN 1.0
                    WHEN 'C.INS' THEN 1.0
                    WHEN 'PCK'   THEN 1.0
                END,
                rm.BaseDays,
                0.0
            ), 2
        ) AS decimal(10,2)) AS DurationDays
    FROM routing_map AS rm
),
routing_cumu AS (
    SELECT rd.*,
           SUM(rd.DurationDays) OVER (
               PARTITION BY rd.prod_order_no, rd.prod_order_lineno, rd.item_no
               ORDER BY rd.SeqNo
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS CumuDaysToThisStep
    FROM routing_dur AS rd
),
 
---------------------------------------------------------------------------
-- 3) FIRST PLATING STEP (+ previous)
---------------------------------------------------------------------------
plating_candidates AS (
    SELECT rc.*,
           ROW_NUMBER() OVER (
              PARTITION BY rc.prod_order_no, rc.prod_order_lineno, rc.item_no
              ORDER BY rc.SeqNo
           ) AS rn_plating
    FROM routing_cumu AS rc
    WHERE rc.MappedGroup  = 'PLATING'
       OR rc.MappedAbbrev = 'PLT'
       OR rc.OpNameU LIKE '%PLAT%'
),
plating_target AS (
    SELECT * FROM plating_candidates WHERE rn_plating = 1
),
prev_before_plating AS (
    SELECT pt.prod_order_no, pt.prod_order_lineno, pt.item_no,
           prev.SeqNo       AS PrevSeqBeforePlating,
           prev.OpName      AS PrevOpNameBeforePlating,
           prev.MappedGroup AS PrevGroupBeforePlating
    FROM plating_target AS pt
    OUTER APPLY (
        SELECT TOP (1) rc.SeqNo, rc.OpName, rc.MappedGroup
        FROM routing_cumu AS rc
        WHERE rc.prod_order_no     = pt.prod_order_no
          AND rc.prod_order_lineno = pt.prod_order_lineno
          AND rc.item_no           = pt.item_no
          AND rc.SeqNo < pt.SeqNo
        ORDER BY rc.SeqNo DESC
    ) AS prev
),
 
---------------------------------------------------------------------------
-- 4) "CURRENT STEP" (numeric → exact-name → partial-name)
---------------------------------------------------------------------------
latest_status AS (
    -- Latest per (order,line,operation_no). Keep textual location too.
    SELECT *
    FROM (
        SELECT
            s.prod_order_no,
            s.prod_order_line_no,
            s.operation_no,
            s.CorrectCurrentLocation AS StatusRoutingNo,
            s.created_on             AS LastSeenAt,
            ROW_NUMBER() OVER (
                PARTITION BY s.prod_order_no, s.prod_order_line_no, s.operation_no
                ORDER BY s.created_on DESC
            ) AS rn
        FROM Gold_Production_Warehouse.prod.gold_production_status AS s
    ) AS x
    WHERE x.rn = 1
),
-- Presence flags (for diagnostics)
status_any_line AS (
    SELECT ls.prod_order_no, ls.prod_order_line_no, 1 AS HasStatus
    FROM latest_status ls
    GROUP BY ls.prod_order_no, ls.prod_order_line_no
),
routing_any_item AS (
    SELECT prod_order_no, prod_order_lineno, item_no, 1 AS HasRouting
    FROM routing_cumu
    GROUP BY prod_order_no, prod_order_lineno, item_no
),
 
numeric_match AS (
    -- PRIMARY: numeric operation_no → SeqNo (simple normalization).
    SELECT rc.*,
           ls.LastSeenAt,
           3 AS MatchRank,
           'NUM' AS MatchType
    FROM routing_cumu AS rc
    JOIN latest_status AS ls
      ON rc.prod_order_no     = ls.prod_order_no
     AND rc.prod_order_lineno = ls.prod_order_line_no
     AND TRY_CONVERT(decimal(18,3), REPLACE(LTRIM(RTRIM(ls.operation_no)), ',', '.')) = rc.SeqNo
),
name_match_exact AS (
    -- SECONDARY: exact textual match to OpNameU or RoutingCode.
    SELECT rc.*,
           ls.LastSeenAt,
           2 AS MatchRank,
           'NAME_EQ' AS MatchType
    FROM routing_cumu AS rc
    JOIN latest_status AS ls
      ON rc.prod_order_no     = ls.prod_order_no
     AND rc.prod_order_lineno = ls.prod_order_line_no
     AND (
           UPPER(LTRIM(RTRIM(ls.StatusRoutingNo))) = rc.OpNameU
        OR UPPER(LTRIM(RTRIM(ls.StatusRoutingNo))) = UPPER(LTRIM(RTRIM(rc.RoutingCode)))
     )
),
name_match_partial AS (
    -- TERTIARY: partial textual match (lowest priority).
    SELECT rc.*,
           ls.LastSeenAt,
           1 AS MatchRank,
           'NAME_LIKE' AS MatchType
    FROM routing_cumu AS rc
    JOIN latest_status AS ls
      ON rc.prod_order_no     = ls.prod_order_no
     AND rc.prod_order_lineno = ls.prod_order_line_no
     AND (
           rc.OpNameU LIKE '%' + UPPER(LTRIM(RTRIM(ls.StatusRoutingNo))) + '%'
        OR UPPER(LTRIM(RTRIM(ls.StatusRoutingNo))) LIKE '%' + rc.OpNameU + '%'
     )
),
current_step AS (
    -- Best match per (order,line,item): prefer higher rank, then latest time, then higher SeqNo.
    SELECT *
    FROM (
        SELECT
            u.prod_order_no,
            u.prod_order_lineno,
            u.item_no,
            u.SeqNo,
            u.OpName,
            u.MappedGroup,
            u.CumuDaysToThisStep,
            u.LastSeenAt,
            u.MatchRank,
            u.MatchType,
            ROW_NUMBER() OVER (
                PARTITION BY u.prod_order_no, u.prod_order_lineno, u.item_no
                ORDER BY u.MatchRank DESC, u.LastSeenAt DESC, u.SeqNo DESC
            ) AS rn
        FROM (
            SELECT * FROM numeric_match
            UNION ALL
            SELECT * FROM name_match_exact
            UNION ALL
            SELECT * FROM name_match_partial
        ) AS u
    ) AS z
    WHERE z.rn = 1
),
current_pick AS (
    SELECT
        prod_order_no,
        prod_order_lineno,
        item_no,
        SeqNo,
        OpName,
        MappedGroup,
        CumuDaysToThisStep,
        LastSeenAt,
        MatchType
    FROM current_step
),
next_from_current AS (
    SELECT cp.prod_order_no, cp.prod_order_lineno, cp.item_no,
           nxt.SeqNo AS NextSeqNo, nxt.OpName AS NextOpName, nxt.MappedGroup AS NextGroup
    FROM current_pick AS cp
    OUTER APPLY (
        SELECT TOP (1) rc.SeqNo, rc.OpName, rc.MappedGroup
        FROM routing_cumu AS rc
        WHERE rc.prod_order_no     = cp.prod_order_no
          AND rc.prod_order_lineno = cp.prod_order_lineno
          AND rc.item_no           = cp.item_no
          AND rc.SeqNo > cp.SeqNo
        ORDER BY rc.SeqNo
    ) AS nxt
),
sum_to_plating AS (
    SELECT pt.prod_order_no, pt.prod_order_lineno, pt.item_no,
           pt.CumuDaysToThisStep AS CumuToPlatingDays,
           pt.SeqNo AS PlatingSeqNo
    FROM plating_target AS pt
),
cumu_before_now AS (
    SELECT cp.prod_order_no, cp.prod_order_lineno, cp.item_no,
           ISNULL(MAX(CASE WHEN rc.SeqNo < cp.SeqNo THEN rc.CumuDaysToThisStep END), 0.0) AS CumuBeforeDays
    FROM current_pick AS cp
    JOIN routing_cumu AS rc
      ON rc.prod_order_no     = cp.prod_order_no
     AND rc.prod_order_lineno = cp.prod_order_lineno
     AND rc.item_no           = cp.item_no
    GROUP BY cp.prod_order_no, cp.prod_order_lineno, cp.item_no
),
 
---------------------------------------------------------------------------
-- 5) PER-ITEM RESULTS + ETA + DIAGNOSTICS (STRING_AGG builder)
---------------------------------------------------------------------------
result_per_item AS (
    SELECT
        l.prod_order_no,
        l.prod_order_line_no,
        l.prod_item_line           AS item_no,
        l.prod_order_quantity,
 
        l.StartDatePlan,
        l.StartDateActual,
 
        ISNULL(rai.HasRouting, 0)  AS HasRouting,
        ISNULL(sal.HasStatus, 0)   AS HasStatus,
 
        pbp.PrevSeqBeforePlating,
        pbp.PrevOpNameBeforePlating,
        pbp.PrevGroupBeforePlating,
 
        stp.CumuToPlatingDays,     -- may be NULL
        stp.PlatingSeqNo,          -- may be NULL
 
        CASE WHEN stp.CumuToPlatingDays IS NOT NULL
             THEN CAST(l.StartDatePlan AS datetime) + CAST(stp.CumuToPlatingDays AS float)
        END AS PlannedPlating_FromOrderStart,
 
        CASE WHEN stp.CumuToPlatingDays IS NOT NULL AND l.StartDateActual IS NOT NULL
             THEN CAST(l.StartDateActual AS datetime) + CAST(stp.CumuToPlatingDays AS float)
        END AS PlannedPlating_FromActualStart,
 
        cp.SeqNo        AS CurrentSeqNo,
        cp.OpName       AS CurrentOpName,
        cp.LastSeenAt   AS StatusLastSeenAt,
        cp.MatchType    AS CurrentMatchType,   -- NUM / NAME_EQ / NAME_LIKE
 
        nfc.NextSeqNo   AS NextSeqNo,
        nfc.NextOpName  AS NextOpName,
 
        CASE
            WHEN cp.SeqNo IS NOT NULL AND stp.CumuToPlatingDays IS NOT NULL
            THEN CASE WHEN stp.CumuToPlatingDays - cbn.CumuBeforeDays < 0
                      THEN 0.0 ELSE stp.CumuToPlatingDays - cbn.CumuBeforeDays END
            ELSE NULL
        END AS RemainToPlatingDays_fromNow,
 
        CASE
            WHEN cp.SeqNo IS NOT NULL AND stp.CumuToPlatingDays IS NOT NULL
            THEN (
                (SELECT MAX(v) FROM (VALUES
                    (CAST(cp.LastSeenAt     AS datetime)),
                    (CAST(l.StartDateActual AS datetime)),
                    (CAST(l.StartDatePlan   AS datetime)),
                    (CAST(GETDATE()         AS datetime))
                 ) AS base(v))
                + CAST(
                    CASE WHEN stp.CumuToPlatingDays - cbn.CumuBeforeDays < 0
                         THEN 0.0 ELSE stp.CumuToPlatingDays - cbn.CumuBeforeDays
                    END AS float)
            )
            ELSE NULL
        END AS ETA_FromCurrentStatus,
 
        CASE
            WHEN stp.PlatingSeqNo IS NULL THEN 'waiting'
            WHEN cp.SeqNo IS NULL THEN 'waiting'
            WHEN (cp.SeqNo > stp.PlatingSeqNo) OR (cbn.CumuBeforeDays >= stp.CumuToPlatingDays)
                 THEN 'finished'
            ELSE 'waiting'
        END AS PlatingStatus,
 
        -- WhyMissing (Synapse-safe STRING_AGG builder)
        diag.WhyMissing
    FROM line AS l
    LEFT JOIN routing_any_item AS rai
      ON rai.prod_order_no     = l.prod_order_no
     AND rai.prod_order_lineno = l.prod_order_line_no
     AND rai.item_no           = l.prod_item_line
    LEFT JOIN status_any_line AS sal
      ON sal.prod_order_no     = l.prod_order_no
     AND sal.prod_order_line_no= l.prod_order_line_no
 
    LEFT JOIN sum_to_plating AS stp
      ON stp.prod_order_no     = l.prod_order_no
     AND stp.prod_order_lineno = l.prod_order_line_no
     AND stp.item_no           = l.prod_item_line
 
    LEFT JOIN prev_before_plating AS pbp
      ON pbp.prod_order_no     = l.prod_order_no
     AND pbp.prod_order_lineno = l.prod_order_line_no
     AND pbp.item_no           = l.prod_item_line
 
    LEFT JOIN current_pick AS cp
      ON cp.prod_order_no      = l.prod_order_no
     AND cp.prod_order_lineno  = l.prod_order_line_no
     AND cp.item_no            = l.prod_item_line
 
    LEFT JOIN next_from_current AS nfc
      ON nfc.prod_order_no     = l.prod_order_no
     AND nfc.prod_order_lineno = l.prod_order_line_no
     AND nfc.item_no           = l.prod_item_line
 
    LEFT JOIN cumu_before_now AS cbn
      ON cbn.prod_order_no     = l.prod_order_no
     AND cbn.prod_order_lineno = l.prod_order_line_no
     AND cbn.item_no           = l.prod_item_line
 
    -- Build WhyMissing using STRING_AGG instead of FOR XML
    OUTER APPLY (
        SELECT
            CASE
                WHEN COUNT(r) = 0 THEN 'OK'
                ELSE STRING_AGG(r, '; ')
            END AS WhyMissing
        FROM (
            VALUES
              (CASE WHEN stp.PlatingSeqNo IS NULL                            THEN 'NO_PLATING_FOUND'   END),
              (CASE WHEN ISNULL(sal.HasStatus,0) = 0                         THEN 'NO_STATUS_AVAILABLE' END),
              (CASE WHEN ISNULL(sal.HasStatus,0) = 1 AND cp.SeqNo IS NULL    THEN 'NO_STATUS_MATCH'     END),
              (CASE WHEN ISNULL(rai.HasRouting,0) = 0                         THEN 'NO_ROUTING'           END),
              (CASE WHEN l.StartDatePlan IS NULL                              THEN 'NO_PLAN_START'        END),
              (CASE WHEN l.StartDateActual IS NULL                            THEN 'NO_ACTUAL_START'      END)
        ) AS reasons(r)
        WHERE r IS NOT NULL
    ) AS diag
),
 
---------------------------------------------------------------------------
-- 6) PICK ONE REPRESENTATIVE ITEM PER (ORDER, LINE)
---------------------------------------------------------------------------
result_per_line AS (
    SELECT *
    FROM (
        SELECT
            rpi.*,
            COALESCE(rpi.PlannedPlating_FromActualStart,
                     rpi.PlannedPlating_FromOrderStart) AS PlannedPlatingForRanking,
            ROW_NUMBER() OVER (
                PARTITION BY rpi.prod_order_no, rpi.prod_order_line_no
                ORDER BY COALESCE(rpi.PlannedPlating_FromActualStart,
                                  rpi.PlannedPlating_FromOrderStart) ASC
            ) AS rn
        FROM result_per_item AS rpi
    ) AS x
    WHERE x.rn = 1
)
 
---------------------------------------------------------------------------
-- 7) FINAL OUTPUT
---------------------------------------------------------------------------
SELECT
    prod_order_no,
    prod_order_line_no,
    prod_order_quantity,
    CONCAT(prod_order_no, '-', prod_order_line_no) AS pol,
    item_no,
 
    StartDatePlan,
    StartDateActual,
 
    PrevSeqBeforePlating,
    PrevOpNameBeforePlating,
    PrevGroupBeforePlating,
 
    CumuToPlatingDays,
 
    PlannedPlating_FromOrderStart,
    PlannedPlating_FromActualStart,
 
    CurrentSeqNo,
    CurrentOpName,
    StatusLastSeenAt,
    NextSeqNo,
    NextOpName,
 
    RemainToPlatingDays_fromNow,
 
    ETA_FromCurrentStatus,
    PlatingStatus,
 
    HasRouting,
    HasStatus,
    CurrentMatchType,
    WhyMissing
FROM result_per_line;