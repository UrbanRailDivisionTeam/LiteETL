SELECT
    r.FID as id,
    r.FBIZDATE AS "时间"
FROM
    ZJEAS7.T_MM_CompletionReport r
    LEFT JOIN ZJEAS7.T_MM_CompletionRAT rt ON r.fid = rt.fparentid
WHERE
    rt.FPERSONNUMBERID is not null AND r.FBIZDATE >= ADD_MONTHS(SYSDATE, -24)