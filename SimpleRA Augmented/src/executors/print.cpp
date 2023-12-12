#include "global.h"
/**
 * @brief 
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINT()
{
    logger.log("syntacticParsePRINT");
    if (tokenizedQuery.size() > 3 || (tokenizedQuery.size() == 3 && tokenizedQuery[1] != "MATRIX"))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = PRINT;
    parsedQuery.printRelationName = (tokenizedQuery.size() == 3) ? tokenizedQuery[2] : tokenizedQuery[1];
    return true;
}

bool semanticParsePRINT()
{
    logger.log("semanticParsePRINT");
    if (!tableCatalogue.isTable(parsedQuery.printRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    return true;
}

void executePRINT()
{
    logger.log("executePRINT");
    Table* table = tableCatalogue.getTable(parsedQuery.printRelationName);

    table->print();
    if (table->isMatrix) {
        cout << "Number of blocks read: " << table->blockCount << endl;
        cout << "Number of blocks written: " << 0 << endl;
        cout << "Number of blocks accessed: " << table->blockCount << endl;
    }
    return;
}
