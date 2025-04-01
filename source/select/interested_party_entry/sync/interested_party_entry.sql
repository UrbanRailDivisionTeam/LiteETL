SELECT
    a.id,
    s."submitTime" AS "提交时间",
    a."formApplicationNumber" AS "来源单据编号",
    a."name" AS "随行人名称",
    a."idNumber" AS "随行人身份证号",
    a."phoneNumber" AS "随行人电话"
FROM
    accompaningpersons a
    LEFT JOIN safeformhead s ON a."formApplicationNumber" = s."applicationNumber"