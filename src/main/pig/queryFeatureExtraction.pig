queries = LOAD '$INPUT/queries';
tempQueryInfo = FOREACH queries { 
    GENERATE (long)$0 AS sessionId, (long)$1 AS timePassed, (long)$3 AS serpId, 
        (long)$4 AS queryId, STRSPLIT((chararray)$5, ',') AS terms, 
        {(0, STRSPLIT((chararray)$6, ',')), (1, STRSPLIT((chararray)$7, ',')), (2, STRSPLIT((chararray)$8, ',')), 
        (3, STRSPLIT((chararray)$9, ',')), (4, STRSPLIT((chararray)$10, ',')), (5, STRSPLIT((chararray)$11, ',')), 
        (6, STRSPLIT((chararray)$12, ',')), (7, STRSPLIT((chararray)$13, ',')), (8, STRSPLIT((chararray)$14, ',')), 
        (9, STRSPLIT((chararray)$15, ','))} AS results;
}

queryInfo = FOREACH tempQueryInfo {
    a = FOREACH results GENERATE $0, FLATTEN($1);
    b = FOREACH a GENERATE (long)$0 AS rank, (long)$1 AS urlId, (long)$2 AS domainId;
    GENERATE sessionId, timePassed, serpId, queryId, terms, b as results;
};

-- capture the query length
queryLength = FOREACH queryInfo GENERATE sessionId, serpId, queryId, SIZE(terms);
STORE queryLength INTO '$OUTPUT/queryLength' USING PigStorage();

-- capture each queries position
queryPosition = FOREACH queryInfo GENERATE sessionId, queryId, serpId;
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
