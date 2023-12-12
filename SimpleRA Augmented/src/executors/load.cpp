#include "global.h"
/**
 * @brief 
 * SYNTAX: LOAD relation_name
 */
bool syntacticParseLOAD()
{
    logger.log("syntacticParseLOAD");
    if (tokenizedQuery.size() > 3 || (tokenizedQuery.size() == 3 && tokenizedQuery[1] != "MATRIX"))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = LOAD;
    parsedQuery.loadRelationName = (tokenizedQuery.size() == 3) ? tokenizedQuery[2] : tokenizedQuery[1];
    return true;
}

bool semanticParseLOAD()
{
    logger.log("semanticParseLOAD");
    if (tableCatalogue.isTable(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Relation already exists" << endl;
        return false;
    }

    if (!isFileExists(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
        return false;
    }
    return true;
}

void executeLOAD()
{
    logger.log("executeLOAD");

    Table *table = new Table(parsedQuery.loadRelationName);
    if (tokenizedQuery[1] == "MATRIX")
        table->isMatrix = true;
    if (table->load())
    {
        tableCatalogue.insertTable(table);
        if (table->isMatrix) {
            cout << "Loaded Matrix. Column Count: " << table->columnCount << " Row Count: " << table->rowCount << endl;
            cout << "Number of blocks read: " << table->blockCount << endl;
            cout << "Number of blocks written: " << table->blockCount << endl;
            cout << "Number of blocks accessed: " << 2*table->blockCount << endl;
        }
        else
            cout << "Loaded Table. Column Count: " << table->columnCount << " Row Count: " << table->rowCount << endl;
    }
    return;
}