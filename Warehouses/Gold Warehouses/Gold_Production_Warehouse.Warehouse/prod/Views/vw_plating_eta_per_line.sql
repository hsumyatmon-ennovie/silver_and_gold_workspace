-- Auto Generated (Do not modify) EF84D54B9FAD5F112107B5B8EFEE8DED415CE127CC0CB358A89E3020EB31665D
CREATE   VIEW prod.vw_plating_eta_per_line
AS
/* ============================================================================
   PURPOSE
     Compute ETA to the first PLATING step per (production order, line),
     pick one representative item per line, and label PlatingStatus from
     the current matched step’s abbreviation.

   BUSINESS RULES (keep/drop)
     • DROP orders with prod_order_no LIKE 'C%'
     • KEEP ONLY routing steps with abbrev in:
       FIL, HT, TUM, LAS, SET, POL, SHI, PLT, GLU, QC, QA, C.INS, PCK
     • KEEP ONLY items that actually have a PLATING step

   STATUS MAPPING (based on *current* step abbrev)
     • waiting     : FIL, HT, TUM, LAS, SET, POL, SHI
     • in_plating  : PLT
     • finished    : GLU, QC, QA, C.INS, PCK

   OUTPUT
     • Current step + next step, remaining days to PLATING, ETA, PlatingStatus
   ============================================================================ */

WITH
/*----------------------------------------------------------------------------
  0) ORDERS/LINE/ITEM & START DATES
     - Only Released; exclude 'C%' orders
     - Keep the most recent planned start per (order,line,item)
-----------------------------------------------------------------------------*/
line0 AS (
    SELECT
        pol.prod_order_no,
        pol.prod_order_line_no,
        pol.prod_item_line,
        pol.prod_order_quantity,
        CONVERT(date, pol.prod_line_start_date) AS StartDatePlan,  -- date-only
        ROW_NUMBER() OVER (
            PARTITION BY pol.prod_order_no, pol.prod_order_line_no, pol.prod_item_line
            ORDER BY pol.prod_line_start_date DESC
        ) AS rn
    FROM Gold_Production_Warehouse.prod.gold_production_order AS pol
    WHERE pol.prod_order_status = 'Released'
      AND pol.prod_order_no NOT LIKE 'C%'           -- <<< drop 'C*' orders here
),
-- earliest status row per (order,line) → Actual Start Date (date-only)
start_from_status AS (
    SELECT
        s.prod_order_no,
        s.prod_order_line_no,
        CONVERT(date, s.created_on) AS StartDateActual
    FROM (
        SELECT s.*,
               ROW_NUMBER() OVER (
                   PARTITION BY s.prod_order_no, s.prod_order_line_no
                   ORDER BY s.operation_no ASC    -- earliest operation number
               ) AS rn
        FROM Silver_Production_Warehouse.prod.silver_production_status AS s
        WHERE s.operation_no IS NOT NULL AND LTRIM(RTRIM(s.operation_no)) <> ''
    ) AS s
    WHERE s.rn = 1
),
line AS (
    SELECT
        l0.prod_order_no,
        l0.prod_order_line_no,
        l0.prod_item_line,
        l0.prod_order_quantity,
        l0.StartDatePlan,
        sfs.StartDateActual
    FROM line0 AS l0
    LEFT JOIN start_from_status AS sfs
      ON sfs.prod_order_no      = l0.prod_order_no
     AND sfs.prod_order_line_no = l0.prod_order_line_no
    WHERE l0.rn = 1
),

/*----------------------------------------------------------------------------
  1) ROUTING → NORMALIZE/MAP
     - Load Machine Center routing lines for kept orders/lines
     - Map to canonical (Group, Abbrev, BaseDays)
-----------------------------------------------------------------------------*/
routing_raw AS (
    SELECT
        prl.prod_order_no,
        prl.prod_order_lineno,
        prl.item_no,
        TRY_CONVERT(decimal(18,3), prl.operation_no) AS SeqNo,   -- '010.01' → 10.01
        COALESCE(NULLIF(LTRIM(RTRIM(mc.current_operation)), ''),  -- prefer mapped name
                 prl.routing_no)                  AS OpName,
        prl.routing_no                            AS RoutingCode,
        UPPER(prl.type_name)                      AS TypeNameU
    FROM Silver_Production_Warehouse.prod.silver_routing_lines AS prl
    LEFT JOIN Silver_Commons_Warehouse.cmn.silver_prod_step_casting_production AS mc
      ON mc.current_operation = prl.routing_no
    INNER JOIN line AS l
      ON prl.prod_order_no     = l.prod_order_no
     AND prl.prod_order_lineno = l.prod_order_line_no
    WHERE UPPER(prl.type_name) = 'MACHINE CENTER'
),
routing_norm AS (
    SELECT
        r.prod_order_no,
        r.prod_order_lineno,
        r.item_no,
        r.SeqNo,
        r.RoutingCode,
        r.OpName,
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
-- Attach mapped Group/Abbrev/BaseDays (exact MapKey match preferred)
routing_map AS (
    SELECT
        rn.*,
        m.OpGroup    AS MappedGroup,
        m.OpAbb      AS MappedAbbrev,
        m.OffsetDays AS BaseDays
    FROM routing_norm AS rn
    OUTER APPLY (
        SELECT TOP (1) om.OpGroup, om.OpAbb, om.OffsetDays
        FROM op_map AS om
        WHERE rn.OpNameU = om.MapKey
           OR rn.OpName  = om.MapKey
           OR om.DescKey LIKE '%' + rn.OpNameU + '%'
        ORDER BY CASE WHEN rn.OpNameU = om.MapKey THEN 0 ELSE 1 END,
                 om.OpAbb, om.OpGroup
    ) AS m
),

/*----------------------------------------------------------------------------
  1.1) ONLY KEEP THESE ABBREVIATIONS (hard filter)
-----------------------------------------------------------------------------*/
allowed_ops AS (
    SELECT 'FIL' AS abbr UNION ALL
    SELECT 'HT'  UNION ALL
    SELECT 'TUM' UNION ALL
    SELECT 'LAS' UNION ALL
    SELECT 'SET' UNION ALL
    SELECT 'POL' UNION ALL
    SELECT 'SHI' UNION ALL
    SELECT 'PLT' UNION ALL
    SELECT 'GLU' UNION ALL
    SELECT 'QC'  UNION ALL
    SELECT 'QA'  UNION ALL
    SELECT 'C.INS' UNION ALL
    SELECT 'PCK'
),
routing_allowed AS (
    SELECT rm.*
    FROM routing_map AS rm
    JOIN allowed_ops AS ao
      ON ao.abbr = UPPER(LTRIM(RTRIM(rm.MappedAbbrev)))
),

/*----------------------------------------------------------------------------
  2) DURATION PER STEP (days) + CUMULATIVE DAYS FROM ORDER START
-----------------------------------------------------------------------------*/
routing_dur AS (
    SELECT
        ra.*,
        CAST(ROUND(
            COALESCE(
                CASE UPPER(NULLIF(ra.MappedAbbrev, ''))
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
                ra.BaseDays,     -- mapping default (already "days")
                0.0
            ), 2
        ) AS decimal(10,2)) AS DurationDays
    FROM routing_allowed AS ra
),
routing_cumu AS (
    SELECT
        rd.*,
        SUM(rd.DurationDays) OVER (
            PARTITION BY rd.prod_order_no, rd.prod_order_lineno, rd.item_no
            ORDER BY rd.SeqNo
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS CumuDaysToThisStep
    FROM routing_dur AS rd
),

/*----------------------------------------------------------------------------
  3) FIND THE FIRST PLATING STEP PER ITEM
     - candidate: any row that looks like PLATING
     - target   : earliest (lowest SeqNo) plating row per item
     - keep only items that actually have plating
-----------------------------------------------------------------------------*/
plating_candidates AS (
    SELECT
        rc.*,
        ROW_NUMBER() OVER (
            PARTITION BY rc.prod_order_no, rc.prod_order_lineno, rc.item_no
            ORDER BY rc.SeqNo
        ) AS rn_plating
    FROM routing_cumu AS rc
    WHERE rc.MappedGroup  = 'PLATING'
       OR UPPER(LTRIM(RTRIM(rc.MappedAbbrev))) = 'PLT'
       OR rc.OpNameU LIKE '%PLAT%'
),
plating_target AS (
    SELECT * FROM plating_candidates WHERE rn_plating = 1
),
line_with_plating AS (
    SELECT l.*
    FROM line AS l
    INNER JOIN plating_target AS pt
      ON pt.prod_order_no     = l.prod_order_no
     AND pt.prod_order_lineno = l.prod_order_line_no
     AND pt.item_no           = l.prod_item_line
),

/*----------------------------------------------------------------------------
  4) LATEST STATUS & MATCH CURRENT STEP
     - Build latest status per (order,line,operation_no)
     - Try 3 matching strategies: numeric > exact name > partial name
     - Keep MappedAbbrev in the chosen current step (for status mapping)
-----------------------------------------------------------------------------*/
latest_status AS (
    SELECT *
    FROM (
        SELECT
            s.prod_order_no,
            s.prod_order_line_no,
            s.operation_no,
            s.CorrectCurrentLocation AS StatusRoutingNo,  -- textual status "name"
            s.created_on             AS LastSeenAt,
            ROW_NUMBER() OVER (
                PARTITION BY s.prod_order_no, s.prod_order_line_no, s.operation_no
                ORDER BY s.created_on DESC
            ) AS rn
        FROM Gold_Production_Warehouse.prod.gold_production_status AS s
    ) AS x
    WHERE x.rn = 1
),
-- PRIMARY: numeric normalization operation_no → decimal(18,3) → SeqNo
numeric_match AS (
    SELECT
        rc.prod_order_no,
        rc.prod_order_lineno,
        rc.item_no,
        rc.SeqNo,
        rc.OpName,
        rc.MappedGroup,
        rc.MappedAbbrev,                 -- keep abbrev
        rc.CumuDaysToThisStep,
        ls.LastSeenAt,
        'NUM' AS MatchType,
        3     AS MatchRank
    FROM routing_cumu AS rc
    JOIN latest_status AS ls
      ON rc.prod_order_no     = ls.prod_order_no
     AND rc.prod_order_lineno = ls.prod_order_line_no
     AND TRY_CONVERT(decimal(18,3), REPLACE(LTRIM(RTRIM(ls.operation_no)), ',', '.')) = rc.SeqNo
),
-- SECONDARY: exact textual match
name_match_exact AS (
    SELECT
        rc.prod_order_no,
        rc.prod_order_lineno,
        rc.item_no,
        rc.SeqNo,
        rc.OpName,
        rc.MappedGroup,
        rc.MappedAbbrev,                 -- keep abbrev
        rc.CumuDaysToThisStep,
        ls.LastSeenAt,
        'NAME_EQ' AS MatchType,
        2         AS MatchRank
    FROM routing_cumu AS rc
    JOIN latest_status AS ls
      ON rc.prod_order_no     = ls.prod_order_no
     AND rc.prod_order_lineno = ls.prod_order_line_no
     AND (
           UPPER(LTRIM(RTRIM(ls.StatusRoutingNo))) = rc.OpNameU
        OR UPPER(LTRIM(RTRIM(ls.StatusRoutingNo))) = UPPER(LTRIM(RTRIM(rc.RoutingCode)))
     )
),
-- TERTIARY: partial textual match
name_match_partial AS (
    SELECT
        rc.prod_order_no,
        rc.prod_order_lineno,
        rc.item_no,
        rc.SeqNo,
        rc.OpName,
        rc.MappedGroup,
        rc.MappedAbbrev,                 -- keep abbrev
        rc.CumuDaysToThisStep,
        ls.LastSeenAt,
        'NAME_LIKE' AS MatchType,
        1           AS MatchRank
    FROM routing_cumu AS rc
    JOIN latest_status AS ls
      ON rc.prod_order_no     = ls.prod_order_no
     AND rc.prod_order_lineno = ls.prod_order_line_no
     AND (
           rc.OpNameU LIKE '%' + UPPER(LTRIM(RTRIM(ls.StatusRoutingNo))) + '%'
        OR UPPER(LTRIM(RTRIM(ls.StatusRoutingNo))) LIKE '%' + rc.OpNameU + '%'
     )
),
-- choose the best current step per item (numeric > exact > partial; then latest time; then highest SeqNo)
current_step AS (
    SELECT *
    FROM (
        SELECT
            u.prod_order_no,
            u.prod_order_lineno,
            u.item_no,
            u.SeqNo,
            u.OpName,
            u.MappedGroup,
            u.MappedAbbrev,               -- << used for PlatingStatus mapping
            u.CumuDaysToThisStep,
            u.LastSeenAt,
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
        MappedAbbrev,                     -- current abbrev for status mapping
        CumuDaysToThisStep,
        LastSeenAt,
        MatchType
    FROM current_step
),
-- Next step after current (if any)
next_from_current AS (
    SELECT
        cp.prod_order_no,
        cp.prod_order_lineno,
        cp.item_no,
        nxt.SeqNo   AS NextSeqNo,
        nxt.OpName  AS NextOpName,
        nxt.MappedGroup AS NextGroup
    FROM current_pick AS cp
    OUTER APPLY (
        SELECT TOP (1)
            rc.SeqNo,
            rc.OpName,
            rc.MappedGroup
        FROM routing_cumu AS rc
        WHERE rc.prod_order_no     = cp.prod_order_no
          AND rc.prod_order_lineno = cp.prod_order_lineno
          AND rc.item_no           = cp.item_no
          AND rc.SeqNo > cp.SeqNo
        ORDER BY rc.SeqNo
    ) AS nxt
),

/*----------------------------------------------------------------------------
  5) SUMS AROUND PLATING
-----------------------------------------------------------------------------*/
sum_to_plating AS (
    SELECT
        pt.prod_order_no,
        pt.prod_order_lineno,
        pt.item_no,
        pt.CumuDaysToThisStep AS CumuToPlatingDays,
        pt.SeqNo              AS PlatingSeqNo
    FROM plating_target AS pt
),
cumu_before_now AS (
    SELECT
        cp.prod_order_no,
        cp.prod_order_lineno,
        cp.item_no,
        ISNULL(MAX(CASE WHEN rc.SeqNo < cp.SeqNo THEN rc.CumuDaysToThisStep END), 0.0)
            AS CumuBeforeDays
    FROM current_pick AS cp
    JOIN routing_cumu AS rc
      ON rc.prod_order_no     = cp.prod_order_no
     AND rc.prod_order_lineno = cp.prod_order_lineno
     AND rc.item_no           = cp.item_no
    GROUP BY cp.prod_order_no, cp.prod_order_lineno, cp.item_no
),

/*----------------------------------------------------------------------------
  6) PER-ITEM RESULT + ETA
-----------------------------------------------------------------------------*/
result_per_item AS (
    SELECT
        lwp.prod_order_no,
        lwp.prod_order_line_no,
        lwp.prod_item_line           AS item_no,
        lwp.prod_order_quantity,
        lwp.StartDatePlan,
        lwp.StartDateActual,

        stp.CumuToPlatingDays,
        stp.PlatingSeqNo,

        CAST(lwp.StartDatePlan AS datetime) + CAST(stp.CumuToPlatingDays AS float)
            AS PlannedPlating_FromOrderStart,
        CASE WHEN lwp.StartDateActual IS NOT NULL
             THEN CAST(lwp.StartDateActual AS datetime) + CAST(stp.CumuToPlatingDays AS float)
        END AS PlannedPlating_FromActualStart,

        cp.SeqNo        AS CurrentSeqNo,
        cp.OpName       AS CurrentOpName,
        cp.LastSeenAt   AS StatusLastSeenAt,
        cp.MappedAbbrev AS CurrentAbbrev,   -- expose for clarity

        nfc.NextSeqNo   AS NextSeqNo,
        nfc.NextOpName  AS NextOpName,

        -- Remaining days to first PLATING from "now/current"
        CASE
            WHEN cp.SeqNo IS NOT NULL
              THEN CASE WHEN stp.CumuToPlatingDays - cbn.CumuBeforeDays < 0
                        THEN 0.0
                        ELSE stp.CumuToPlatingDays - cbn.CumuBeforeDays
                   END
            ELSE NULL
        END AS RemainToPlatingDays_fromNow,

        -- ETA = max(LastSeenAt, GETDATE()) + remaining days
        CASE
            WHEN cp.SeqNo IS NOT NULL
              THEN DATEADD(
                       day,
                       CASE WHEN stp.CumuToPlatingDays - cbn.CumuBeforeDays < 0
                            THEN 0.0
                            ELSE stp.CumuToPlatingDays - cbn.CumuBeforeDays
                       END,
                       CASE WHEN cp.LastSeenAt > GETDATE() THEN cp.LastSeenAt ELSE GETDATE() END
                   )
            ELSE NULL
        END AS ETA_FromCurrentStatus,

        -- PlatingStatus logic driven by *current* abbrev
        CASE
            WHEN cp.MappedAbbrev IS NULL THEN 'waiting' -- no match → treat as not yet reached
            WHEN UPPER(LTRIM(RTRIM(cp.MappedAbbrev))) IN ('FIL','HT','TUM','LAS','SET','POL','SHI') THEN 'waiting'
            WHEN UPPER(LTRIM(RTRIM(cp.MappedAbbrev))) = 'PLT' THEN 'in_plating'
            WHEN UPPER(LTRIM(RTRIM(cp.MappedAbbrev))) IN ('GLU','QC','QA','C.INS','PCK') THEN 'finished'
            ELSE 'waiting'  -- fallback
        END AS PlatingStatus

    FROM line_with_plating AS lwp

    -- totals to plating
    INNER JOIN sum_to_plating AS stp
      ON stp.prod_order_no     = lwp.prod_order_no
     AND stp.prod_order_lineno = lwp.prod_order_line_no
     AND stp.item_no           = lwp.prod_item_line

    -- current step (may be NULL when no status match)
    LEFT JOIN current_pick AS cp
      ON cp.prod_order_no      = lwp.prod_order_no
     AND cp.prod_order_lineno  = lwp.prod_order_line_no
     AND cp.item_no            = lwp.prod_item_line

    -- next step after current
    LEFT JOIN next_from_current AS nfc
      ON nfc.prod_order_no     = lwp.prod_order_no
     AND nfc.prod_order_lineno = lwp.prod_order_line_no
     AND nfc.item_no           = lwp.prod_item_line

    -- cumulative days consumed before "now"
    LEFT JOIN cumu_before_now AS cbn
      ON cbn.prod_order_no     = lwp.prod_order_no
     AND cbn.prod_order_lineno = lwp.prod_order_line_no
     AND cbn.item_no           = lwp.prod_item_line
),

/*----------------------------------------------------------------------------
  7) PICK ONE REPRESENTATIVE ITEM PER (ORDER, LINE)
     - Choose the item with earliest planned plating (actual-start preferred)
-----------------------------------------------------------------------------*/
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

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================
SELECT
    prod_order_no,
    prod_order_line_no,
    prod_order_quantity,
    CAST(prod_order_no + N'-' + CAST(prod_order_line_no AS NVARCHAR(50)) AS NVARCHAR(256)) AS pol,
    item_no,

    StartDatePlan,
    StartDateActual,

    CumuToPlatingDays,

    PlannedPlating_FromOrderStart,
    PlannedPlating_FromActualStart,

    CurrentSeqNo,
    CurrentOpName,
    StatusLastSeenAt,
    CurrentAbbrev,

    NextSeqNo,
    NextOpName,

    RemainToPlatingDays_fromNow,
    ETA_FromCurrentStatus,
    PlatingStatus
FROM result_per_line;