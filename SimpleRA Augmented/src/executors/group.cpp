#include "global.h"
/**
 * @brief File contains method to process GROUP BY commands.
 * 
 * SYNTAX: <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> HAVING <aggregate(attribute)> <bin_op> <attribute_value> RETURN <aggregate_func(attribute)>
 */
bool syntacticParseGROUP(){
    logger.log("syntacticParseGROUP");
    int n = tokenizedQuery.size();
    if (tokenizedQuery[3] != "BY" || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "HAVING" || tokenizedQuery[n-2] != "RETURN") {
        cout<<"SYNTAX ERROR1"<<endl;
        return false;
    }
    
    parsedQuery.queryType = GROUP;
    parsedQuery.groupColumn = tokenizedQuery[4];
    parsedQuery.groupResultRelationName = tokenizedQuery[0];
    parsedQuery.groupSourceRelationName = tokenizedQuery[6];

    unordered_set<string> aggregateFunctions{"MAX", "MIN", "SUM", "AVG", "COUNT"};
    unordered_set<string> binaryOperators{"==", ">", ">=", "<", "<="};
    int start = 0, end = 0;

    start = tokenizedQuery[8].find("(");
    if (start == string::npos) {
        cout<<"SYNTAX ERROR2"<<endl;
        return false;
    }
    string agg1 = tokenizedQuery[8].substr(0, start);
    end = tokenizedQuery[8].find(")");
    if (aggregateFunctions.find(agg1) == aggregateFunctions.end() || end == string::npos) {
        cout<<"SYNTAX ERROR3"<<endl;
        return false;
    }
    parsedQuery.groupAggFunction1 = agg1;
    parsedQuery.groupAggColumn1 = tokenizedQuery[8].substr(start+1, end-start-1);

    string binOp = tokenizedQuery[9];
    if (binaryOperators.find(binOp) == binaryOperators.end()) {
        cout<<"SYNTAX ERROR4"<<endl;
        return false;
    }
    parsedQuery.groupBinaryOperator = binOp;

    string value = tokenizedQuery[10];
    for (int i = 11; i < n-2; i++) {
        value += tokenizedQuery[i];
    }
    parsedQuery.groupCondValue = stoi(value);
    // end = value.find(",");
    // if (end != string::npos) {
    //     value = value.substr(0, end) + value.substr(end+1, value.size()-end-1);
    // } 

    start = tokenizedQuery[n-1].find("(");
    if (start == string::npos) {
        cout<<"SYNTAX ERROR5"<<endl;
        return false;
    }
    string agg2 = tokenizedQuery[n-1].substr(0, start);
    end = tokenizedQuery[n-1].find(")");
    if (aggregateFunctions.find(agg2) == aggregateFunctions.end() || end == string::npos) {
        cout<<"SYNTAX ERROR6"<<endl;
        return false;
    }
    parsedQuery.groupAggFunction2 = agg2;
    parsedQuery.groupAggColumn2 = tokenizedQuery[n-1].substr(start+1, end-start-1);

    parsedQuery.groupResultColumn = parsedQuery.groupAggFunction2 + parsedQuery.groupAggColumn2;

    return true;
}

bool semanticParseGROUP(){
    logger.log("semanticParseGROUP");

    if(tableCatalogue.isTable(parsedQuery.groupResultRelationName)){
        cout<<"SEMANTIC ERROR: Resultant relation already exists"<<endl;
        return false;
    }

    if(!tableCatalogue.isTable(parsedQuery.groupSourceRelationName)){
        cout<<"SEMANTIC ERROR: Relation doesn't exist"<<endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.groupColumn, parsedQuery.groupSourceRelationName)){
        cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.groupAggColumn1, parsedQuery.groupSourceRelationName)){
        cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.groupAggColumn2, parsedQuery.groupSourceRelationName)){
        cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
        return false;
    }

    return true;
}

void executeGROUP()
{
    logger.log("executeGROUP");

    Table* originalTable = tableCatalogue.getTable(parsedQuery.groupSourceRelationName);
    Table* table = new Table();
    table->columnCount = originalTable->columnCount;
    table->columns = originalTable->columns;
    table->maxRowsPerBlock = originalTable->maxRowsPerBlock;
    table->tableName = "SORT_TEMP_" + parsedQuery.groupSourceRelationName;
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

    // Table* table = tableCatalogue.getTable(parsedQuery.groupSourceRelationName);
    table->sortMerge({table->getColumnIndex(parsedQuery.groupColumn)}, {"ASC"}, "TEMP_" + parsedQuery.groupSourceRelationName, parsedQuery.groupSourceRelationName);

    Table* newTable = new Table(parsedQuery.groupResultRelationName);
    newTable->columns = {parsedQuery.groupColumn, parsedQuery.groupResultColumn};
    newTable->columnCount = newTable->columns.size();
    newTable->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * newTable->columnCount));

    table->group(parsedQuery.groupColumn, parsedQuery.groupAggFunction1, parsedQuery.groupAggColumn1, parsedQuery.groupBinaryOperator, parsedQuery.groupCondValue, parsedQuery.groupAggFunction2, parsedQuery.groupAggColumn2, parsedQuery.groupResultColumn, newTable);
    tableCatalogue.insertTable(newTable);

    tableCatalogue.deleteTable(table->tableName);

    return;
}