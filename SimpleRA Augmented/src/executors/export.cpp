#include "global.h"

/**
 * @brief 
 * SYNTAX: EXPORT <relation_name> 
 */

bool syntacticParseEXPORT()
{
    logger.log("syntacticParseEXPORT");
    if (tokenizedQuery.size() > 3 || (tokenizedQuery.size() == 3 && tokenizedQuery[1] != "MATRIX"))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = EXPORT;
    parsedQuery.exportRelationName = (tokenizedQuery.size() == 3) ? tokenizedQuery[2] : tokenizedQuery[1];
    return true;
}

bool semanticParseEXPORT()
{
    logger.log("semanticParseEXPORT");
    //Table should exist
    if (tableCatalogue.isTable(parsedQuery.exportRelationName))
        return true;
    cout << "SEMANTIC ERROR: No such relation exists" << endl;
    return false;
}

void executeEXPORT()
{
    logger.log("executeEXPORT");
    Table* table = tableCatalogue.getTable(parsedQuery.exportRelationName);
    table->makePermanent();
    if (table->isMatrix) {
        cout << "Number of blocks read: " << table->blockCount << endl;
        cout << "Number of blocks written: " << table->blockCount << endl;
        cout << "Number of blocks accessed: " << 2*table->blockCount << endl;
    }
    return;
}