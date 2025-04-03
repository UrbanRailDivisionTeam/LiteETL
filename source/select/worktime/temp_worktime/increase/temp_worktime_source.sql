SELECT
    CONCAT (f.ID, F.iOrd) AS "id",
    p.ModifyDate AS "修改日期"
FROM
    tabDIYTable3292 f
    LEFT JOIN tabDIYTable3290 p ON f.ID = p.ID