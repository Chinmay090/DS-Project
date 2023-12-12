#include "global.h"
/**
 * @brief 
 * SYNTAX: CHECKSYMMETRY matrix_name
 */

bool syntacticParseCHECKSYMMETRY()
{
    logger.log("syntacticParseCHECKSYMMETRY");
    if (tokenizedQuery.size() > 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CHECKSYMMETRY;
    parsedQuery.checksymmetrymatrixName = tokenizedQuery[1];
    return true;
}

bool semanticParseCHECKSYMMETRY()
{
    logger.log("semanticParseCHECKSYMMETRY");
    if (!tableCatalogue.isMatrix(parsedQuery.checksymmetrymatrixName))
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
 * Checksymmetry requires us to check each element (i,j) for all i < j in the upper triangular section of the matrix 
 * against its corresponding mirrored element (j,i) to check if it is symmetrical
 * Constraints on the maxRowsPerBlock means that at the most, we have to deal with data across 250 blocks
 * 
 * Implementing a naive solution for now, we iterate through all required (i,j)s, retrieving the associated block as
 * needed. We are only allowed two blocks of storage in main memory, so in the worst case we store the two separate blocks
 * that contain (i,j) and (j,i)
 * To minimise the amount of loading and unloading of blocks we process all (i,j)s in one block first
 * 
 * A symmetric matrix HAS to be the same height and width

*/
void executeCHECKSYMMETRY()
{
    logger.log("executeCHECKSYMMETRY");
    string tableName = parsedQuery.checksymmetrymatrixName;
    Table* table = tableCatalogue.getTable(tableName);

    int blocksRead = 0, blocksWritten = 0, blocksAccessed = 0;
    bool symmetrical = true;

    if(table->rowCount != table->columnCount){
        cout << "FALSE" << endl;

        cout << "Number of blocks read: " << blocksRead << endl;
        cout << "Number of blocks written: " << blocksWritten << endl;
        cout << "Number of blocks accesssed: " << blocksAccessed << endl;
        return;
    }

    // if columnCount <= maxRowsPerBlock => only retreive one block,
    if(table->rowCount <= table->maxRowsPerBlock){
        // access the only block
        Cursor onlyPageCursor(tableName, 0);
        blocksRead++; blocksAccessed++;

        // read the entire page
        vector<vector<int>> pageData;
        for(int i = 0; i < table->columnCount; i++)
            pageData.push_back(onlyPageCursor.getNext());

        // check symmetry
        for(int i = 0; i < pageData.size() && symmetrical; i++)
            for(int j = i; j < pageData[0].size() && symmetrical; j++){
                if(pageData[i][j] != pageData[j][i]) symmetrical = false;
            }
    }

    // else, retrieve ` ceil(columnCount / maxRowsPerBlock) ` pages,
    else{
        // maintain two pageData variables, load them as needed
        vector<vector<int>> queryPageData, targetPageData;
        int queryIndex = -1, targetIndex = -1;

        for(int i = 0; i < table->columnCount && symmetrical; i++){
            for(int j = i; j < table->columnCount && symmetrical; j++){
                // load blocks if a new page is needed
                int queryReq = ceil(float(table->columnCount)/table->maxColumnsPerBlock) * (i/table->maxRowsPerBlock) + (j/table->maxColumnsPerBlock); // page that contains the query
                if(queryReq != queryIndex){
                    blocksRead++; blocksAccessed++;

                    Cursor c(tableName, queryReq);
                    queryPageData.clear();
                    for(int i = 0; i < table->columnCount; i++)
                        queryPageData.push_back(c.getNext());
                    
                    queryIndex = queryReq;
                }

                // do the same for target
                int targetReq = ceil(float(table->columnCount)/table->maxColumnsPerBlock) * (j/table->maxRowsPerBlock) + (i/table->maxColumnsPerBlock); // page that contains the query
                if(targetReq != targetIndex){
                    blocksRead++; blocksAccessed++;

                    Cursor c(tableName, targetReq);
                    targetPageData.clear();
                    for(int k = 0; k < table->columnCount; k++)
                        targetPageData.push_back(c.getNext());
                    
                    targetIndex = targetReq;
                }

                if(queryPageData[i % table->maxRowsPerBlock][j % table->maxColumnsPerBlock] != targetPageData[j % table->maxRowsPerBlock][i % table->maxColumnsPerBlock]) 
                    symmetrical = false;
            }
        }   
    }

    if(symmetrical)
        cout << "TRUE" << endl;
    else
        cout << "FALSE" << endl;

    cout << "Number of blocks read: " << blocksRead << endl;
    cout << "Number of blocks written: " << blocksWritten << endl;
    cout << "Number of blocks accesssed: " << blocksAccessed << endl;
    
    return;
}
