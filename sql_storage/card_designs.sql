SELECT `_id`,
z.`cardType`,
z.`designCode`,
z.`designName`,
z.`designTitle`,
z.`isAvailable`,
z.`filePath`,
from_unixtime(CAST(ca.`createdAt` / 1000 AS BIGINT)) as `createdAt`,
from_unixtime(CAST(ua.`updatedAt` / 1000 AS BIGINT)) as `updatedAt`,
y.`issuanceFee` as `fee_issuancefee`,
y.`replacementFee` as `fee_replacementFee`,
z.`replacementFeeCounter`,
z.`numberOfArrangement`, buss_date
from %s.card_designs
LATERAL VIEW json_tuple(json,'cardType', 'designCode', 'designName', 'designTitle', 'isAvailable', 'filePath'
, 'createdAt', 'updatedAt', 'fee', 'replacementFeeCounter','numberOfArrangement') z
as `cardType`, `designCode`, `designName`, `designTitle`, `isAvailable`, `filePath`
, `createdAt`, `updatedAt`, `fee`, `replacementFeeCounter`,`numberOfArrangement`
LATERAL VIEW json_tuple(z.`fee`,'issuanceFee','replacementFee') y as `issuanceFee`,`replacementFee`
lateral view json_tuple(z.`createdAt`,'$date') ca as createdAt
lateral view json_tuple(z.`updatedAt`,'$date') ua as updatedAt
WHERE 1=1