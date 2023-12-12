#include "global.h"
/**
 * @brief
 * SYNTAX: TRANSPOSE MATRIX matrix_name
 */

bool syntacticParseTRANSPOSE()
{
    logger.log("syntacticParseTRANSPOSE");
    if (tokenizedQuery.size() != 3 || tokenizedQuery[1] != "MATRIX")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = TRANSPOSE;
    parsedQuery.transposeMatrixName = tokenizedQuery[2];
    return true;
}

bool semanticParseTRANSPOSE()
{
    logger.log("semanticParseTRANSPOSE");
    if (!tableCatalogue.isMatrix(parsedQuery.transposeMatrixName))
    {
        cout << "SEMANTIC ERROR: Matrix doesn't exist" << endl;
        return false;
    }
    return true;
}

/*
 * Because of maxRowsPerBlock being calculated with a constant factor of 1000*blocksize(=1), our maximum number of cols
 * that allow for even 1 row per block is 250
 *
 * Each page has all the columns of the table, and at most table->maxRowsPerBlock, which ranges from 1-250 given
 * the default block size of 1
 *
 * Using a similar scheme to access tables as checksym, we manually edit the page table stores
 * Whenever a page is switched out of main memory, we write it to that same page
 * At the end of execution, both stored tables are written
 * All of our transposes occur element by element in local memory. All writes to pages happen only when pages are
 * switched out or at the end of execution
 *
 * Then, we update the tables local variables to reflect the new number of columns
 *
 * ASSUMING THAT ALL PROVIDED MAATRICES ARE SQUARE

*/
void executeTRANSPOSE()
{
    logger.log("executeTRANSPOSE");
    string tableName = parsedQuery.transposeMatrixName;
    Table* table = tableCatalogue.getTable(tableName);

    int blocksRead = 0, blocksWritten = 0;
    
    // maintain two pageData variables, load them as needed
    vector<vector<int>> queryPageData, targetPageData;
    int queryIndex = -1, targetIndex = -1;

    for(int i = 0; i < table->columnCount; i++){
        for(int j = i; j < table->columnCount; j++){

            // load blocks if a new page is needed
            int queryReq = ceil(float(table->columnCount)/table->maxColumnsPerBlock) * (i/table->maxRowsPerBlock) + (j/table->maxColumnsPerBlock);  // page that contains the query
            if(queryReq != queryIndex){
                // save progress so far, write to file
                if (queryIndex != -1)
                {
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

            // update our locally stored blocks
            if(queryIndex == targetIndex){
                int temp = queryPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock];
                queryPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] = queryPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock];
                queryPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock] = temp;

                temp = targetPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock];
                targetPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] = targetPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock];
                targetPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock] = temp;
            }
            else{
                int temp = queryPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock];
                queryPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] = targetPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock];
                targetPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock] = temp;
            }
        }
    }

    // final write to pages
    if(queryIndex != -1){
        Page toWrite(tableName, queryIndex, queryPageData, queryPageData.size());
        toWrite.writePage();
        blocksWritten++;
    }

    if(targetIndex != -1){
        Page toWrite(tableName, targetIndex, targetPageData, targetPageData.size());
        toWrite.writePage();
        blocksWritten++;
    }

    // flush outdated, loaded pages
    bufferManager.flushPool();

    cout << "Number of blocks read: " << blocksRead << endl;
    cout << "Number of blocks written: " << blocksWritten << endl;
    cout << "Number of blocks accesssed: " << blocksRead + blocksWritten << endl;

    return;
}
