#include "global.h"
/**
 * @brief 
 * SYNTAX: COMPUTE matrix_name
 */

bool syntacticParseCOMPUTE()
{
    logger.log("syntacticParseCOMPUTE");
    if (tokenizedQuery.size() > 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = COMPUTE;
    parsedQuery.computematrixName = tokenizedQuery[1];
    return true;
}

bool semanticParseCOMPUTE()
{
    logger.log("semanticParseCOMPUTE");
    if (!tableCatalogue.isMatrix(parsedQuery.computematrixName))
    {
        cout << "SEMANTIC ERROR: Matrix doesn't exist" << endl;
        return false;
    }
    return true;
}

/*
 * The result table will take exactly the same amount of space to store as the original table
 * Thus the number of pages will be exactly the same
 * Iterate through the top triangular half of the table, 

*/
void executeCOMPUTE()
{
    logger.log("executeCOMPUTE");
    string tableName = parsedQuery.computematrixName;
    Table* table = tableCatalogue.getTable(tableName);

    string newTableName = tableName + "_RESULT";
    Table newTable = Table(table);
    
    newTable.tableName = newTableName;
    newTable.sourceFileName = "";
    newTable.isMatrix = true;
    newTable.columns = vector<string>(table->columnCount, "eat shit");

    Table* nTable = &newTable;
    tableCatalogue.insertTable(&newTable);

    Table* C = tableCatalogue.getTable(tableName);

    int blocksRead = 0, blocksWritten = 0;
    
    // maintain two pageData variables, load them as needed
    vector<vector<int>> queryPageData, targetPageData, pageToWrite, pageToWriteB;
    int queryIndex = -1, targetIndex = -1, writeIndex = -1, writeIndexB = -1;

    // fill out result pages with zeros
    int numPages = ceil(float(table->columnCount)/table->maxColumnsPerBlock);
    for(int i = 0; i < numPages * numPages; i++){
        // bottom row
        if (i >= (numPages - 1)*numPages){
            if ((i+1)%numPages == 0){
                pageToWrite = vector<vector<int>>(table->rowCount % table->maxRowsPerBlock, vector<int>(table->columnCount % table->maxColumnsPerBlock, 0));
            }
            else
                pageToWrite = vector<vector<int>>(table->rowCount % table->maxRowsPerBlock, vector<int>(table->maxColumnsPerBlock, 0));
        }
        // right edge
        else if ((i+1)%numPages == 0){
            pageToWrite = vector<vector<int>>(table->maxRowsPerBlock, vector<int>(table->columnCount % table->maxColumnsPerBlock, 0));
        }
        else{
            pageToWrite = vector<vector<int>>(table->maxRowsPerBlock, vector<int>(table->maxColumnsPerBlock, 0));
        }
        Page toWrite(newTableName, i, pageToWrite, pageToWrite.size());
        toWrite.writePage();
        blocksWritten++;
    }

    for(int i = 0; i < table->columnCount; i++){
        for(int j = i; j < table->columnCount; j++){
            // load blocks if a new page is needed
            int queryReq = ceil(float(table->columnCount)/table->maxColumnsPerBlock) * (i/table->maxRowsPerBlock) + (j/table->maxColumnsPerBlock);  // page that contains the query
            if(queryReq != queryIndex){
                // save progress so far, write to file
                if(queryIndex != -1){
                    Page toWrite(tableName, queryIndex, queryPageData, queryPageData.size());
                    toWrite.writePage();
                    blocksWritten++;
                }

                Cursor c(tableName, queryReq);
                queryPageData.clear();
                for(int k = 0; 
                    k < (((i/table->maxColumnsPerBlock) != ceil(float(table->columnCount)/table->maxColumnsPerBlock) - 1) ? (table->maxColumnsPerBlock) : (table->columnCount % table->maxColumnsPerBlock));
                    k++)
                    queryPageData.push_back(c.getNext());
                blocksRead++;
                
                queryIndex = queryReq;
            }

            // write page
            if(queryReq != writeIndex){
                // save progress so far, write to file
                if(writeIndex != -1){
                    Page toWrite(newTableName, writeIndex, pageToWrite, pageToWrite.size());
                    // toWrite.writePage();
                    blocksWritten++;
                }

                Cursor c(newTableName, queryReq);
                pageToWrite.clear();
                for(int k = 0; 
                    k < (((i/nTable->maxColumnsPerBlock) != ceil(float(nTable->columnCount)/nTable->maxColumnsPerBlock) - 1) ? (nTable->maxColumnsPerBlock) : (nTable->columnCount % nTable->maxColumnsPerBlock));
                    k++)
                    pageToWrite.push_back(c.getNext());
                blocksRead++;
                
                writeIndex = queryReq;
            }

            // do the same for target
            int targetReq = ceil(float(table->columnCount)/table->maxColumnsPerBlock) * (j/table->maxRowsPerBlock) + (i/table->maxColumnsPerBlock);
            if(targetReq != targetIndex){
                // save progress so far, write to file
                if(targetIndex != -1){
                    Page toWrite(tableName, targetIndex, targetPageData, targetPageData.size());
                    toWrite.writePage();
                    blocksWritten++;
                }

                Cursor c(tableName, targetReq);
                targetPageData.clear();
                for(int k = 0; 
                    k < (((j/table->maxColumnsPerBlock) != ceil(float(table->columnCount)/table->maxColumnsPerBlock) - 1) ? (table->maxColumnsPerBlock) : (table->columnCount % table->maxColumnsPerBlock));
                    k++)
                    targetPageData.push_back(c.getNext());
                blocksRead++;
                
                targetIndex = targetReq;
            }

            // write page
            if(targetReq != writeIndexB){
                // save progress so far, write to file
                if(writeIndexB != -1){
                    Page toWrite(newTableName, writeIndexB, pageToWriteB, pageToWriteB.size());
                    toWrite.writePage();
                    blocksWritten++;
                }

                Cursor c(newTableName, targetReq);
                pageToWriteB.clear();
                for(int k = 0; 
                    k < (((j/nTable->maxColumnsPerBlock) != ceil(float(nTable->columnCount)/nTable->maxColumnsPerBlock) - 1) ? (nTable->maxColumnsPerBlock) : (nTable->columnCount % nTable->maxColumnsPerBlock));
                    k++)
                    pageToWriteB.push_back(c.getNext());
                blocksRead++;
                
                writeIndexB = targetReq;
            }

            // update our locally stored blocks
            if(queryIndex == targetIndex){
                pageToWrite[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] = queryPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] - targetPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock];
                pageToWrite[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock] = queryPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock] - targetPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock];

                pageToWriteB[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] = queryPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] - targetPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock];
                pageToWriteB[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock] = queryPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock] - targetPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock];
            
            }
            else{
                pageToWrite[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] = queryPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] - targetPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock];
                pageToWriteB[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock] = queryPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock] - targetPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock];
            }
        }
    }

    // final write to pages
    if(queryIndex != -1){
        Page toWrite(newTableName, writeIndex, pageToWrite, pageToWrite.size());
        toWrite.writePage();
        blocksWritten++;
    }

    if(targetIndex != -1){
        Page toWrite(newTableName, writeIndexB, pageToWriteB, pageToWriteB.size());
        toWrite.writePage();
        blocksWritten++;
    }

    // flush outdated, loaded pages
    bufferManager.flushPool();

    cout << "Number of blocks read: " << blocksRead << endl;
    cout << "Number of blocks written: " << blocksWritten << endl;
    cout << "Number of blocks accesssed: " << blocksRead + blocksWritten << endl;
    
    C = tableCatalogue.getTable(tableName);

    return;
}
