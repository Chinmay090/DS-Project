#include "global.h"
/**
 * @brief 
 * SYNTAX: RENAME column_name TO column_name FROM relation_name
 */
bool syntacticParseRENAME()
{
    logger.log("syntacticParseRENAME");
    if (tokenizedQuery.size() != 6 && tokenizedQuery.size() != 4) 
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (tokenizedQuery.size() == 6 && (tokenizedQuery[2] != "TO" || tokenizedQuery[4] != "FROM"))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (tokenizedQuery.size() == 4 && tokenizedQuery[1] != "MATRIX") 
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = RENAME;
    if (tokenizedQuery.size() == 6) {
        parsedQuery.renameFromColumnName = tokenizedQuery[1];
        parsedQuery.renameToColumnName = tokenizedQuery[3];
        parsedQuery.renameRelationName = tokenizedQuery[5];
    } else if (tokenizedQuery.size() == 4) {
        parsedQuery.renameFromMatrixName = tokenizedQuery[2];
        parsedQuery.renameToMatrixName = tokenizedQuery[3];
    }
    return true;
}

bool semanticParseRENAME()
{
    logger.log("semanticParseRENAME");

    if (tokenizedQuery.size() == 6) {
        if (!tableCatalogue.isTable(parsedQuery.renameRelationName))
        {
            cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
            return false;
        }

        if (!tableCatalogue.isColumnFromTable(parsedQuery.renameFromColumnName, parsedQuery.renameRelationName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
            return false;
        }

        if (tableCatalogue.isColumnFromTable(parsedQuery.renameToColumnName, parsedQuery.renameRelationName))
        {
            cout << "SEMANTIC ERROR: Column with name already exists" << endl;
            return false;
        }
    } else if (tokenizedQuery.size() == 4) {
        if (!tableCatalogue.isTable(parsedQuery.renameFromMatrixName))
        {
            cout << "SEMANTIC ERROR: Matrix doesn't exist" << endl;
            return false;
        }

        if (tableCatalogue.isTable(parsedQuery.renameToMatrixName))
        {
            cout << "SEMANTIC ERROR: Matrix with name already exists" << endl;
            return false;
        }
    }
    
    return true;
}

void executeRENAME()
{
    logger.log("executeRENAME");
    Table* table = NULL;
    if (tokenizedQuery.size() == 6)
    {
        table = tableCatalogue.getTable(parsedQuery.renameRelationName);
        table->renameColumn(parsedQuery.renameFromColumnName, parsedQuery.renameToColumnName);
    }
    else if (tokenizedQuery.size() == 4)
    {
        tableCatalogue.renameMatrix(parsedQuery.renameFromMatrixName, parsedQuery.renameToMatrixName);
    }
    
    return;
}