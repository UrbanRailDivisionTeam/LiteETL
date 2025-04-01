SELECT
    a.id,
    s."submitTime" AS "提交时间"
FROM
    accompaningpersons a
    LEFT JOIN safeformhead s ON a."formApplicationNumber" = s."applicationNumber"