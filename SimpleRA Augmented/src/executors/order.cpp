#include "global.h"
/**
 * @brief File contains method to process ORDER BY commands.
 * 
 * SYNTAX: <new_table> <- ORDER BY <attribute> ASC|DESC ON <table_name>
 */
bool syntacticParseORDER(){
    logger.log("syntacticParseORDER");
    
    if (tokenizedQuery.size() != 8 || tokenizedQuery[3] != "BY" || tokenizedQuery[6] != "ON") {
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    if (tokenizedQuery[5] != "ASC" && tokenizedQuery[5] != "DESC") {
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    parsedQuery.queryType = ORDER;
    parsedQuery.sortRelationName = tokenizedQuery[7];
    parsedQuery.sortResultRelationName = tokenizedQuery[0];

    vector<string> columnNames{tokenizedQuery[4]};
    vector<string> sortingStrategies{tokenizedQuery[5]};
    parsedQuery.sortingStrategies = sortingStrategies;
    parsedQuery.sortColumnNames = columnNames;
    return true;
}

bool semanticParseORDER(){
    logger.log("semanticParseORDER");

    if(tableCatalogue.isTable(parsedQuery.sortResultRelationName)){
        cout<<"SEMANTIC ERROR: Resultant relation already exists"<<endl;
        return false;
    }

    if(!tableCatalogue.isTable(parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Relation doesn't exist"<<endl;
        return false;
    }

    for (string col : parsedQuery.sortColumnNames) {
        if(!tableCatalogue.isColumnFromTable(col, parsedQuery.sortRelationName)){
            cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
            return false;
        }
    }

    return true;
}

void executeORDER(){
    logger.log("executeORDER");
    Table* originalTable = tableCatalogue.getTable(parsedQuery.sortRelationName);
    for (string col : parsedQuery.sortColumnNames)
        parsedQuery.sortColumnIdxs.push_back(originalTable->getColumnIndex(col));

    // Table* table = new Table(originalTable);
    Table* table = new Table();
    table->columnCount = originalTable->columnCount;
    table->columns = originalTable->columns;
    table->maxRowsPerBlock = originalTable->maxRowsPerBlock;
    table->tableName = parsedQuery.sortResultRelationName;
    table->blockCount = 0;
    table->sourceFileName = originalTable->sourceFileName;
    bufferManager.flushPool();
    if (!table->blockify())
    {
        cout << "SEMANTIC ERROR: Not enough memory to perform operation" << endl;
        return;
    }

    table->sourceFileName = "../data/temp/" + table->tableName + ".csv";
    tableCatalogue.insertTable(table);
    
    table->sortMerge(parsedQuery.sortColumnIdxs, parsedQuery.sortingStrategies, "TEMP_" + parsedQuery.sortResultRelationName, parsedQuery.sortResultRelationName);
    // tableCatalogue.renameMatrix(parsedQuery.sortRelationName, parsedQuery.sortResultRelationName);
    // table->makePermanent();
    return;
}