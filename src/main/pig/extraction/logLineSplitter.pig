rawLogs = LOAD '$INPUT';
SPLIT rawLogs INTO metadata IF $1 == 'M', clicks IF $2 == 'C', queries IF $2 == 'Q';

STORE metadata INTO '$OUTPUT/metadata' USING PigStorage();
STORE clicks INTO '$OUTPUT/clicks' USING PigStorage();
STORE queries INTO '$OUTPUT/queries' USING PigStorage();
