queries = LOAD '$INPUT/queries';
queryInfo = FOREACH queries GENERATE (long)$0 AS sessionId, (long)$1 AS timePassed, (long)$3 AS serpId, 
    (long)$4 AS queryId, (chararray)$5 AS terms;

-- capture the query length
queryLength = FOREACH queryInfo GENERATE queryId, SIZE(STRSPLIT(terms, ','));
STORE queryLength INTO '$OUTPUT/queryLength' USING PigStorage();

-- capture each queries position
queryPosition = FOREACH queryInfo GENERATE queryId, serpId;
STORE queryPosition INTO '$OUTPUT/queryPosition' USING PigStorage();

/*======== prior query count in session ========*/

-- capture 'numberOfQueries' for the session scope
queriesGroupedBySession = GROUP queryInfo BY sessionId;

-- create a view of queryInfo where serpId is renamed to currentSerpId to avoid later naming conflicts
queryInfoCurrentSerpId = FOREACH queryInfo GENERATE queryId, sessionId, serpId AS currentSerpId;

-- join all queries with their associate session and queries-within-session
queriesJoinedWithAllQueriesInSession = JOIN queryInfoCurrentSerpId BY sessionId, queriesGroupedBySession BY group;

-- insert the currentSerpId into each of the associate session queries so that we can later filter to only prior queries
projectedJoinedQueries = FOREACH queriesJoinedWithAllQueriesInSession {
    info = FOREACH queriesGroupedBySession::queryInfo GENERATE queryId, queriesJoinedWithAllQueriesInSession.currentSerpId;
    GENERATE queryId, info;
};

-- flatten the query -> session query mapping so that we can filter to only prior queries
flattenedQueries = FOREACH queriesJoinedWithAllQueriesInSession GENERATE queryInfoCurrentSerpId::queryId,
    queryInfoCurrentSerpId::sessionId, queryInfoCurrentSerpId::currentSerpId, FLATTEN(queriesGroupedBySession::queryInfo);

-- filter to only prior queries
priorQueries = FILTER flattenedQueries BY queryInfoCurrentSerpId::currentSerpId > queriesGroupedBySession::queryInfo::serpId;

-- group the prior queries by parent query
priorQueriesGrouped = GROUP priorQueries BY queryInfoCurrentSerpId::queryId;

-- capture the number of distinct prior queries
priorQueryCount = FOREACH priorQueriesGrouped {
    tempPriorQueries = FOREACH priorQueries GENERATE queriesGroupedBySession::queryInfo::queryId;
    distinctPriorQueries = DISTINCT tempPriorQueries.queryId;
    GENERATE group AS queryId, COUNT(distinctPriorQueries);
};

STORE priorQueryCount INTO '$OUTPUT/priorQueryCount' USING PigStorage();
