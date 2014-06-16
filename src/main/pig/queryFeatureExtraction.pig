queries = LOAD '$INPUT/queries';
queryInfo = FOREACH queries GENERATE (long)$0 AS sessionId, (long)$1 AS timePassed, (long)$3 AS serpId, (long)$4 AS queryId, (chararray)$5 AS terms;

-- capture the query length
queryLength = FOREACH queryInfo GENERATE queryId, SIZE(STRSPLIT(terms, ','));
STORE queryLength INTO '$OUTPUT/queryLength' USING PigStorage();

-- capture 'numberOfQueries' for the session scope
queriesGroupedBySession = GROUP queryInfo BY sessionId;

grunt> history                                                                                                               
1   queries = load 'queries';
2   queryInfo = FOREACH queries GENERATE (long)$0 AS sessionId, (long)$1 AS timePassed, (long)$3 AS serpId, (long)$4 AS queryId, (chararray)$5 AS terms;
3   queriesGroupedBySession = GROUP queryInfo BY sessionId;
4   combined = join queryInfo by sessionId, queriesGroupedBySession by group;
5   A = foreach combined {
currentSerpId = queryInfo::serpId;
B = filter queriesGroupedBySession::queryInfo by serpId < 1;
generate queryId, B;
};
6   A = foreach combined {
currentSerpId = queryInfo::serpId;
B = filter queriesGroupedBySession::queryInfo by serpId < 3;
generate queryId, B;
};
7   A = foreach combined {
currentSerpId = serpId;
B = filter queriesGroupedBySession::queryInfo by serpId < currentSerpId;
generate queryId, B;
};
8   A = foreach combined {
currentSerpId = serpId;
B = filter queriesGroupedBySession::queryInfo by serpId < currentSerpId;
generate queryId, currentSerpId, B;
};
9   A = foreach combined {
currentSerpId = serpId;
C = foreach queriesGroupedBySession::queryInfo generate queryId, currentSerpId;
generate queryId, C;
};
10   B = foreach queryInfo generate queryId, sessionId, serpId AS currentSerpId;
11   B = foreach queryInfo generate queryId, sessionId, serpId AS currentSerpId;
12   combined = join B by sessionId, queriesGroupedBySession by group;
13   A = foreach combined {
C = foreach queriesGroupedBySession::queryInfo generate queryId, combined.currentSerpId;
generate queryId, C;
};
14   C = foreach combined generate B::queryId, flatten(queriesGroupedBySession::queryInfo);
15   C = foreach combined generate B::queryId, flatten(queriesGroupedBySession::queryInfo);
16   C = foreach combined generate B::queryId, B::sessionId, B::currentSerpId, flatten(queriesGroupedBySession::queryInfo);
17   D = filter C by B::currentSerpId > queriesGroupedBySession::queryInfo::serpId;
18   E = group D by B::queryId;

