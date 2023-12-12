#include "global.h"
/**
 * @brief File contains method to process SORT commands.
 * 
 * SYNTAX:
 * R <- SORT relation_name BY column_name IN sorting_order - OLD
 * SORT relation_name BY <column_name1, column_name2, ...> IN <sorting_order1, sorting_order2, ...> - NEW
 * 
 * sorting_order = ASC | DESC 
 */
bool syntacticParseSORT(){
    logger.log("syntacticParseSORT");
    
    // if(tokenizedQuery.size()!= 8 || tokenizedQuery[4] != "BY" || tokenizedQuery[6] != "IN"){
    //     cout<<"SYNTAX ERROR"<<endl;
    //     return false;
    // }
    // parsedQuery.queryType = SORT;
    // parsedQuery.sortResultRelationName = tokenizedQuery[0];
    // parsedQuery.sortRelationName = tokenizedQuery[3];
    // parsedQuery.sortColumnName = tokenizedQuery[5];
    // string sortingStrategy = tokenizedQuery[7];
    // if(sortingStrategy == "ASC")
    //     parsedQuery.sortingStrategy = ASC;
    // else if(sortingStrategy == "DESC")
    //     parsedQuery.sortingStrategy = DESC;
    // else{
    //     cout<<"SYNTAX ERROR"<<endl;
    //     return false;
    // }

    // check for (column name, order pairing), min token length, presence of BY and there being atleast one column name
    if (tokenizedQuery.size() % 2 != 0 || tokenizedQuery.size() < 6 || tokenizedQuery[2] != "BY" || tokenizedQuery[3] == "IN") {
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
    
    parsedQuery.queryType = SORT;
    parsedQuery.sortRelationName = tokenizedQuery[1];
    parsedQuery.sortResultRelationName = "TEMP_" + tokenizedQuery[1];

    // parse column names, additional syntax error check
    vector<string> columnNames{};
    int i = 0, j = 0;
    for (i = 3; i < tokenizedQuery.size(); i++) {
        string token = tokenizedQuery[i];
        if (token == "IN")
            break;
        if (token[token.size() - 1] == ',')
            token = token.substr(0, token.size() - 1);
        columnNames.push_back(token);
    }
    if (i >= tokenizedQuery.size() - 1) {
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }

    // parse sorting types names, additional syntax error check
    vector<string> sortingStrategies{};
    for (j = i+1; j < tokenizedQuery.size(); j++) {
        string token = tokenizedQuery[j];
        if (token[token.size() - 1] == ',')
            token = token.substr(0, token.size() - 1);
        if (token != "ASC" && token != "DESC") {
            cout<<"SYNTAX ERROR"<<endl;
            return false;
        }
        sortingStrategies.push_back(token);
    }
    if (columnNames.size() != sortingStrategies.size()) {
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }

    parsedQuery.sortingStrategies = sortingStrategies;
    parsedQuery.sortColumnNames = columnNames;
    return true;
}

bool semanticParseSORT(){
    logger.log("semanticParseSORT");

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

void executeSORT(){
    logger.log("executeSORT");
    Table* table = tableCatalogue.getTable(parsedQuery.sortRelationName);
    for (string col : parsedQuery.sortColumnNames)
        parsedQuery.sortColumnIdxs.push_back(table->getColumnIndex(col));
    table->sortMerge(parsedQuery.sortColumnIdxs, parsedQuery.sortingStrategies, parsedQuery.sortResultRelationName, parsedQuery.sortRelationName);
    table->makePermanent();
    bufferManager.flushPool();
    return;
}