queries = LOAD 'queries';
queryInfo = FOREACH queries GENERATE (long)$0 AS sessionId, (long)$1 AS timePassed, (long)$3 AS serpId, (long)$4 AS queryId, (chararray)$5 AS terms;

-- capture the query length
queryLength = FOREACH queryInfo GENERATE queryId, SIZE(STRSPLIT(terms, ','));

-- capture the distict query count per session
sessionQueryInfo = GROUP queryInfo BY sessionId;
numberOfQueriesSession = FOREACH sessionQueryInfo {
    id = queryInfo.queryId;
    distinctId = DISTINCT id;
    GENERATE group, COUNT(distinctId);
}
