SELECT
    r.FID as id,
    r.FBIZDATE AS "时间"
FROM
    ZJEAS7.T_MM_CompletionReport r
WHERE
    EXISTS (
        SELECT
            1
        FROM
            ZJEAS7.T_MM_CompletionRAT rt
        WHERE
            rt.fparentid = r.fid
            AND rt.FPERSONNUMBERID IS NOT NULL
    )
    AND r.FBIZDATE >= ADD_MONTHS (SYSDATE, -24)