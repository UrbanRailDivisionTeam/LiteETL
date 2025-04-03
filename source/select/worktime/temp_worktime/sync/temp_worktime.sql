SELECT
    CONCAT (f.ID, F.iOrd) AS "id",
    CASE
        WHEN LEN (f.F28273) = 5 THEN CONCAT ('0102000', f.F28273)
        ELSE f.F28273
    END AS "员工编码",
    f.F28273 as "111",
    f.F28274 AS "姓名",
    p.F28264 AS "作业日期",
    p.ModifyDate AS "修改日期",
    f.F34267 AS "批准工时"
FROM
    tabDIYTable3292 f
    LEFT JOIN tabDIYTable3290 p ON f.ID = p.ID