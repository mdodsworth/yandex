rawLogs = LOAD '$INPUT' AS (sessionId:int, typeOfRecord:chararray, day:int, userId:int);
metadata = FILTER rawLogs BY typeOfRecord == 'M';
STORE metadata INTO '$OUTPUT/metadata.log' USING PigStorage();
