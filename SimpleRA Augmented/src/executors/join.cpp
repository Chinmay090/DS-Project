#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 bin_op column_name2
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 9 || tokenizedQuery[5] != "ON")
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[3];
    parsedQuery.joinSecondRelationName = tokenizedQuery[4];
    parsedQuery.joinFirstColumnName = tokenizedQuery[6];
    parsedQuery.joinSecondColumnName = tokenizedQuery[8];

    string binaryOperator = tokenizedQuery[7];
    if (binaryOperator == "<")
        parsedQuery.joinBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.joinBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.joinBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.joinBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.joinBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.joinBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

/*
General strategy:

1. sort both tables based on the respective columns

2. create a new empty table for the result, keep adding shit to it page by page

3. get indexes for second table for each page

4. iterate through all first table rows

5. when a unique value for the 1st row column is found, scan the second table for all pages that 
potentially have values to join. keep this list of pages

5.5. change rules to determine which blocks are to be used based on comparator

6. iterate through all these pages, add rows as necessary

7. keep going through first table rows, if a new value is seen, get a new set of blocks


*/
void executeJOIN()
{
    logger.log("executeJOIN");

    // step 1
    Table* table1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table* table2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);

    Table* temp = NULL;
    if(table1->rowCount > table2->rowCount){
        temp = table2;
        table2 = table1;
        table1 = temp;
    }

    int colID1 = table1->getColumnIndex(parsedQuery.joinFirstColumnName);
    int colID2 = table2->getColumnIndex(parsedQuery.joinSecondColumnName);

    bufferManager.flushPool();
    
    table1->sortMerge(vector<int>{colID1},
                      vector<string>{"ASC"},
                      "TEMP_" + table1->tableName,
                      table1->tableName);

    bufferManager.flushPool();

    table2->sortMerge(vector<int>{colID2},
                      vector<string>{"ASC"},
                      "TEMP_" + table2->tableName,
                      table2->tableName);
    
    // table1->makePermanent();
    // table2->makePermanent();
    bufferManager.flushPool();

    // step 2
    vector<string> joinedColumns = table1->columns;
    joinedColumns.insert(joinedColumns.end(), table2->columns.begin(), table2->columns.end());
    Table* joinedTable = NULL;
    joinedTable = new Table(parsedQuery.joinResultRelationName, joinedColumns);

    joinedTable->isMatrix = false;
    joinedTable->columnCount = joinedColumns.size();

    // step 3
    vector<int> valuePerPage(table2->blockCount, 0);
    Cursor pageScanner(table2->tableName, 0);
    
    // get indices
    for(int blockNum=0; blockNum < table2->blockCount; blockNum++){
        vector<int> row = pageScanner.getNext();
        valuePerPage[blockNum] = row[colID2];

        if(blockNum < table2->blockCount - 1)
            pageScanner.nextPage(blockNum + 1);
    }


    // step 4, begin filling result table
    vector<int> row1;
    Cursor cursor1(table1->tableName, 0);

    vector<int> pagesToBeChecked;
    int lastTable1Value = -2147483648;
    
    vector<vector<int>> resultPageData;
    int resultPageRowCount = 0;
    int resultPageIndex = 0;
    int resultTotalRowCount = 0;
    vector<uint> rowsPerResultBlock;
        
    for(int rowcount1 = 0; rowcount1 < table1->rowCount; rowcount1++){
        row1 = cursor1.getNext();
        // step 5

        // new table 1 value, 
        // check retreived tbl2 values to determine which pages are to be checked
        // based on operator
        if(row1[colID1] != lastTable1Value){
            lastTable1Value = row1[colID1];
            pagesToBeChecked.clear();

            // iterate through all page indices
            for(int i = 0; i < valuePerPage.size(); i++){
                bool isFirst = i == 0;
                bool isLast = i == (valuePerPage.size() - 1);
                int value = valuePerPage[i];

                // step 5.5
                switch (parsedQuery.joinBinaryOperator)
                {            
                    case GREATER_THAN:
                        if(valuePerPage[i] < lastTable1Value)
                            pagesToBeChecked.push_back(i);
                        break;

                    case GEQ:
                        if(valuePerPage[i] <= lastTable1Value)
                            pagesToBeChecked.push_back(i);
                        break;

                    case LESS_THAN:
                        if(valuePerPage[i] > lastTable1Value)
                            pagesToBeChecked.push_back(i);
                        
                        if(!isLast && valuePerPage[i] <= lastTable1Value && valuePerPage[i+1] > lastTable1Value)
                            pagesToBeChecked.push_back(i);
                        break;

                    case LEQ:
                        if(valuePerPage[i] >= lastTable1Value)
                            pagesToBeChecked.push_back(i);
                        
                        if(!isLast && valuePerPage[i] < lastTable1Value && valuePerPage[i+1] >= lastTable1Value)
                            pagesToBeChecked.push_back(i);
                        break;

                    case EQUAL:
                        if(valuePerPage[i] == lastTable1Value)
                            pagesToBeChecked.push_back(i);
                        
                        if(!isLast && valuePerPage[i] < lastTable1Value && valuePerPage[i+1] >= lastTable1Value)
                            pagesToBeChecked.push_back(i);
                        break;

                    case NOT_EQUAL:
                        if(!isLast && valuePerPage[i] == lastTable1Value && valuePerPage[i+1] == lastTable1Value)
                            continue;
                        else
                            pagesToBeChecked.push_back(i);
                        break;

                    case NO_BINOP_CLAUSE:
                    default:
                        std::cerr << ("JOIN CLAUSE ERROR");
                        assert(false);
                        break;
                }
            }

            // check selected pages
            // std::cout << "New value: " << lastTable1Value << " || ";
            // for(auto val : pagesToBeChecked){
            //     std::cout << val << "|" << valuePerPage[val] << " ";
            // }
            // std::cout << '\n';
        }


        // step 6
        // create a row for each valid row in these retrieved table, add to result table
        for(auto checkIdx : pagesToBeChecked){
            Cursor checkCursor(table2->tableName, checkIdx);
            while(checkCursor.pageIndex == checkIdx){
                vector<int> row2 = checkCursor.getNext();
                if(row2.empty()) break;

                int table2Value = row2[colID2];

                bool addRow = false;

                switch (parsedQuery.joinBinaryOperator)
                {            
                    case LESS_THAN:
                        addRow = lastTable1Value < table2Value;
                        break;

                    case LEQ:
                        addRow = lastTable1Value <= table2Value;
                        break;

                    case GREATER_THAN:
                        addRow = lastTable1Value > table2Value;
                        break;

                    case GEQ:
                        addRow = lastTable1Value >= table2Value;
                        break;

                    case EQUAL:
                        addRow = lastTable1Value == table2Value;
                        break;

                    case NOT_EQUAL:
                        addRow = lastTable1Value != table2Value;
                        break;

                    case NO_BINOP_CLAUSE:
                    default:
                        std::cerr << ("JOIN CLAUSE ERROR");
                        assert(false);
                        break;
                }

                // join condition satisifed, add to table
                if(addRow){
                    vector<int> newRow(table1->columnCount + table2->columnCount, 0);
                    int rc = 0;
                    for(auto a : row1)
                        newRow[rc++] = a;
                    for(auto a : row2)
                        newRow[rc++] = a;

                    resultPageData.push_back(newRow);
                    resultPageRowCount++;
                    resultTotalRowCount++;
                }

                // page full, add to table
                if(resultPageRowCount == joinedTable->maxRowsPerBlock){
                    bufferManager.writePage(joinedTable->tableName, resultPageIndex, resultPageData, resultPageRowCount);
                    resultPageData.clear();
                    rowsPerResultBlock.push_back(uint(resultPageRowCount));

                    resultPageRowCount = 0;
                    resultPageIndex++;
                }
            }
        }

        // create a page for leftover rows
        if(!resultPageData.empty()){
            bufferManager.writePage(joinedTable->tableName, resultPageIndex, resultPageData, resultPageRowCount);
            rowsPerResultBlock.push_back(uint(resultPageRowCount));
        }
    }

    joinedTable->rowCount = resultTotalRowCount;
    joinedTable->rowsPerBlockCount = rowsPerResultBlock;

    tableCatalogue.insertTable(joinedTable);
    std::cout << "Join result stored in " << joinedTable->tableName << '\n';
    // std::cout << t->columnCount << '\n';

    bufferManager.flushPool();

    // joinedTable->makePermanent();

    return;
}