queries = LOAD '$INPUT/queries';
queryInfo = FOREACH queries GENERATE (long)$0 AS sessionId, (long)$1 AS timePassed, (long)$3 AS serpId, (long)$4 AS queryId, (chararray)$5 AS terms;

-- capture the query length
queryLength = FOREACH queryInfo GENERATE queryId, SIZE(STRSPLIT(terms, ','));
STORE queryLength INTO '$OUTPUT/queryLength' USING PigStorage();

-- capture 'numberOfQueries' for the session scope
queriesGroupedBySession = GROUP queryInfo BY sessionId;

-- create a view of queryInfo where serpId is renamed to currentSerpId to avoid later naming conflicts
queryInfoCurrentSerpId = foreach queryInfo generate queryId, sessionId, serpId AS currentSerpId;

-- join all queries with their associate session and queries-within-session
queriesJoinedWithAllQueriesInSession = join queryInfoCurrentSerpId by sessionId, queriesGroupedBySession by group;

-- insert the currentSerpId into each of the associate session queries so that we can later filter to only prior queries
projectedJoinedQueries = foreach queriesJoinedWithAllQueriesInSession {
    info = foreach queriesGroupedBySession::queryInfo generate queryId, queriesJoinedWithAllQueriesInSession.currentSerpId;
    generate queryId, info;
};

-- flatten the query -> session query mapping so that we can filter to only prior queries
flattenedQueries = foreach queriesJoinedWithAllQueriesInSession generate queryInfoCurrentSerpId::queryId,
    queryInfoCurrentSerpId::sessionId, queryInfoCurrentSerpId::currentSerpId, flatten(queriesGroupedBySession::queryInfo);

-- filter to only prior queries
priorQueries = filter flattenedQueries by queryInfoCurrentSerpId::currentSerpId > queriesGroupedBySession::queryInfo::serpId;

-- group the prior queries by parent query
priorQueriesGrouped = group priorQueries by queryInfoCurrentSerpId::queryId;
