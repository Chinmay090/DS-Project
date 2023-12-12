#include "global.h"

/**
 * @brief Construct a new Table:: Table object
 *
 */
Table::Table()
{
    logger.log("Table::Table");
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName 
 */
Table::Table(string tableName)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/" + tableName + ".csv";
    this->tableName = tableName;
}

/**
 * @brief Construct a new Table:: Table object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName 
 * @param columns 
 */
Table::Table(string tableName, vector<string> columns)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/temp/" + tableName + ".csv";
    this->tableName = tableName;
    this->columns = columns;
    this->columnCount = columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount));
    this->writeRow<string>(columns);
}

Table::Table(Table *temp)
{
    logger.log("Table::Table");
    this->sourceFileName = temp->sourceFileName;
    this->tableName = temp->tableName;
    this->columns = temp->columns;
    this->columnCount = temp->columnCount;
    this->maxRowsPerBlock = temp->maxRowsPerBlock;
    this->rowCount = temp->rowCount;
    this->blockCount = temp->blockCount;
    this->maxColumnsPerBlock = temp->maxColumnsPerBlock;
    this->rowsPerBlockCount = temp->rowsPerBlockCount;
    this->columnsPerBlockCount = temp->columnsPerBlockCount;
    this->indexed = temp->indexed;
    this->indexedColumn = temp->indexedColumn;
    this->indexingStrategy = temp->indexingStrategy;
    this->isMatrix = temp->isMatrix;
    this->blocksPerRow = temp->blocksPerRow;

    // this->writeRow<string>(temp->columns);
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded 
 * @return false if an error occurred 
 */
bool Table::load()
{
    logger.log("Table::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();

        if (this->isMatrix && this->extractMatrixParams(line) && this->blockifyMatrix())
            return true;
        else if (this->extractColumnNames(line) && this->blockify())
            return true;
    }
    fin.close();
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file. 
 *
 * @param line 
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(string firstLine)
{
    logger.log("Table::extractColumnNames");
    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);
    while (getline(s, word, ','))
    {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        if (columnNames.count(word))
            return false;
        columnNames.insert(word);
        this->columns.emplace_back(word);
    }
    this->columnCount = this->columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    return true;
}

/**
 * @brief Function extracts matrix params from the header line of the .csv data
 * file. 
 *
 * @param line 
 * @return true if matrix parameters are successfully read from first line
 * @return false otherwise
 */
bool Table::extractMatrixParams(string firstLine)
{
    logger.log("Table::extractMatrixParams");
    string word;
    stringstream s(firstLine);
    uint colCount = 0;
    while (getline(s, word, ','))
    {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        colCount++;
    }
    this->columnCount = colCount;
    // this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    this->maxRowsPerBlock = (uint)floor(sqrt((BLOCK_SIZE * 1000) / sizeof(int)));
    this->maxColumnsPerBlock = this->maxRowsPerBlock;
    this->blocksPerRow = this->columnCount / this->maxRowsPerBlock;
    if (this->columnCount % this->maxRowsPerBlock != 0)
        this->blocksPerRow++;
    ifstream fin(this->sourceFileName, ios::in);
    string line;
    while (getline(fin, line))
        this->rowCount++;
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify()
{
    logger.log("Table::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    vector<int> row(this->columnCount, 0);
    vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row);
    int pageCounter = 0;
    unordered_set<int> dummy;
    dummy.clear();
    this->distinctValuesInColumns.assign(this->columnCount, dummy);
    this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
    if (!this->isMatrix)
        getline(fin, line);
    while (getline(fin, line))
    {
        stringstream s(line);
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!getline(s, word, ','))
                return false;
            row[columnCounter] = stoi(word);
            rowsInPage[pageCounter][columnCounter] = row[columnCounter];
        }
        pageCounter++;
        this->updateStatistics(row);
        if (pageCounter == this->maxRowsPerBlock)
        {
            bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
            this->blockCount++;
            this->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }
    }
    if (pageCounter)
    {
        bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
        this->blockCount++;
        this->rowsPerBlockCount.emplace_back(pageCounter);
        pageCounter = 0;
    }

    if (this->rowCount == 0)
        return false;
    // this->distinctValuesInColumns.clear();
    return true;
}

  
bool Table::blockifyMatrix()
{
    logger.log("Table::blockifyMatrix");

    int pageCounter = 0;
    for (int a = 0; a < this->blocksPerRow; a++)
        for (int b = 0; b < this->blocksPerRow; b++) 
        {
            ifstream fin(this->sourceFileName, ios::in);
            string line, word;
            
            vector<vector<int>> rowsInPage;
            pageCounter = 0;

            int rowOffset = a * this->maxRowsPerBlock;
            for (int r = 0; r < rowOffset; r++)
                getline(fin, line);
            
            int rowLimit = this->maxRowsPerBlock;
            if (a == this->blocksPerRow - 1)
                rowLimit = this->rowCount - rowOffset;
            vector<int> row;
            for (int rowCounter = 0; rowCounter < rowLimit; rowCounter++)
            {
                row.clear();
                getline(fin, line);
                stringstream s(line);

                int columnOffset = b * this->maxColumnsPerBlock;
                for (int c = 0; c < columnOffset; c++)
                    if (!getline(s, word, ','))
                        return false;

                int columnLimit = this->maxColumnsPerBlock;
                if (b == this->blocksPerRow - 1)
                    columnLimit = this->columnCount - columnOffset;
                for (int columnCounter = 0; columnCounter < columnLimit; columnCounter++)
                {
                    if (!getline(s, word, ','))
                        return false;
                    row.emplace_back(stoi(word));
                }
                
                rowsInPage.emplace_back(row);
                pageCounter++;
            }
            if (pageCounter)
            {
                bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
                this->blockCount++;
                this->rowsPerBlockCount.emplace_back(pageCounter);
                this->columnsPerBlockCount.emplace_back(rowsInPage[0].size());
                pageCounter = 0;
            }
        }

    if (this->rowCount == 0)
        return false;
    return true;
}

/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation.
 *
 * @param row 
 */
void Table::updateStatistics(vector<int> row)
{
    this->rowCount++;
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (!this->distinctValuesInColumns[columnCounter].count(row[columnCounter]))
        {
            this->distinctValuesInColumns[columnCounter].insert(row[columnCounter]);
            this->distinctValuesPerColumnCount[columnCounter]++;
        }
    }
}

/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName 
 * @return true 
 * @return false 
 */
bool Table::isColumn(string columnName)
{
    logger.log("Table::isColumn");
    for (auto col : this->columns)
    {
        if (col == columnName)
        {
            return true;
        }
    }
    return false;
}

/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName 
 * @param toColumnName 
 */
void Table::renameColumn(string fromColumnName, string toColumnName)
{
    logger.log("Table::renameColumn");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (columns[columnCounter] == fromColumnName)
        {
            columns[columnCounter] = toColumnName;
            break;
        }
    }
    return;
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Table::print()
{
    logger.log("Table::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    string t = "_RESULT";

    if (this->tableName.length() > t.length()) {
        if(this->tableName.compare(this->tableName.length() - t.length(), t.length(), t) == 0){
            this->isMatrix = true;
        }
    }

    std::cout << "b\n";

    //print headings
    if (!this->isMatrix){
        this->writeRow(this->columns, cout);
    }

    std::cout << "c\n";

    if (!this->isMatrix) {
        Cursor cursor(this->tableName, 0);
        vector<int> row;
        for (int rowCounter = 0; rowCounter < count; rowCounter++) {
            row = cursor.getNext();
            this->writeRow(row, cout);
        }
    }
    else {
        for (int rowCounter = 0; rowCounter < count; rowCounter++) {
            int rowPrint = 0;
            int offset = (rowCounter / this->maxRowsPerBlock) * (this->columnCount / this->maxColumnsPerBlock + 1);
            for (int columnPage = 0; columnPage < ((this->columnCount / this->maxColumnsPerBlock) + 1); columnPage++) {
                int pageIdx = offset + columnPage;

                Cursor cursor(this->tableName, pageIdx, rowCounter % this->maxRowsPerBlock, true);
                vector<int> row;

                row = cursor.getNext();

                if(rowPrint + row.size() > count){
                    int s  = int(count - rowCount -1);
                    for(int i = 0; i + rowPrint < count; i++){
                        cout << row[i] << ", ";
                    }
                    break;
                }
                else{
                    this->writeRow(row, cout);
                    if (columnPage != this->columnCount / this->maxColumnsPerBlock)
                        cout << ", ";
                    rowPrint += row.size();
                }
            }
            cout << endl;
        }
    }

    printRowCount(this->rowCount);
}

/**
 * @brief This function returns one row of the table using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor 
 * @return vector<int> 
 */
void Table::getNextPage(Cursor *cursor)
{
    logger.log("Table::getNext");

        if (cursor->pageIndex < this->blockCount - 1)
        {
            cursor->nextPage(cursor->pageIndex+1);
        }
}

/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Table::makePermanent()
{
    logger.log("Table::makePermanent");
    if(!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    //print headings
    if (!this->isMatrix)
        this->writeRow(this->columns, fout);

    if (!this->isMatrix) 
    {
        Cursor cursor(this->tableName, 0);
        vector<int> row;
        for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
        {
            row = cursor.getNext();
            this->writeRow(row, fout);
        }
    } 
    else 
    {
        for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++) {
            int offset = (rowCounter / this->maxRowsPerBlock) * (this->columnCount / this->maxColumnsPerBlock + 1);
            for (int columnPage = 0; columnPage < ((this->columnCount / this->maxColumnsPerBlock) + 1); columnPage++) {
                int pageIdx = offset + columnPage;
                Cursor cursor(this->tableName, pageIdx, rowCounter % this->maxRowsPerBlock, true);
                vector<int> row;
                row = cursor.getNext();
                this->writeRow(row, fout);
                if (columnPage != this->columnCount / this->maxColumnsPerBlock)
                    fout << ", ";
            }
            fout << endl;
        }
    }
    
    fout.close();
}

/**
 * @brief Function to check if table is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Table::isPermanent()
{
    logger.log("Table::isPermanent");
    if (this->sourceFileName == "../data/" + this->tableName + ".csv")
    return true;
    return false;
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table
 *
 */
void Table::unload(){
    logger.log("Table::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->tableName, pageCounter);
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}

/**
 * @brief Function that returns a cursor that reads rows from this table
 * 
 * @return Cursor 
 */
Cursor Table::getCursor()
{
    logger.log("Table::getCursor");
    Cursor cursor(this->tableName, 0);
    return cursor;
}
/**
 * @brief Function that returns the index of column indicated by columnName
 * 
 * @param columnName 
 * @return int 
 */
int Table::getColumnIndex(string columnName)
{
    logger.log("Table::getColumnIndex");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == columnName)
            return columnCounter;
    }
}

/**
 * @brief Function that sorts the rows on the columns indicated by columnIndexes
 * 
 * @param columnIndexes
 * @param sortingStrategies
 * @param rowPairs
 * 
 * @return int value of the index of the first row pair
 */
int Table::comparator(vector<int> &columnIndexes, vector<string> &sortingStrategies, vector<pair<vector<int>, int>> &rowPairs)
{
    // Define a comparison function for the sort
    auto compareRows = [columnIndexes, sortingStrategies](const pair<vector<int>, int> &a, const pair<vector<int>, int> &b) {
        for (int j = 0; j < columnIndexes.size(); j++) {
            int colIndex = columnIndexes[j];
            if (a.first[colIndex] != b.first[colIndex]) {
                if (sortingStrategies[j] == "ASC") {
                    return a.first[colIndex] < b.first[colIndex];
                } else {
                    return a.first[colIndex] > b.first[colIndex];
                }
            }
        }
        return false;
    };

    // Sort the rows using the comparison function
    sort(rowPairs.begin(), rowPairs.end(), compareRows);

    // Return the index of first row
    return rowPairs[0].second;
}

/**
 * @brief Function that sorts the table on the columns indicated by columnIndexes
 * 
 * @param sortingStrategy 
 * @param columnIndexes 
 * @param tempName
 * @param originalName
 */
void Table::sortMerge(vector<int> columnIndexes, vector<string> sortingStrategies, string tempName, string originalName) 
{
    logger.log("Table::sortMerge");
    bufferManager.flushPool();

    // Sort and merge each run in level 0.
    int numRuns = this->blockCount/BLOCK_COUNT;

    // For table with multiple pages > BLOCK_COUNT
    for (int i = 0; i < numRuns; i++) 
    {
        // Sort BLOCK_COUNT number of pages in each run
        vector<vector<vector<int>>> run;
        for (int pageCounter = i*BLOCK_COUNT; pageCounter < (i+1)*BLOCK_COUNT; pageCounter++) 
        {
            vector<vector<int>> rowsInPage = bufferManager.getPage(originalName, pageCounter).getRows();
            sort(rowsInPage.begin(), rowsInPage.end(), [columnIndexes, sortingStrategies](vector<int> &a, vector<int> &b) {
                int j = 0;
                for (int columnCounter : columnIndexes) 
                {
                    if (a[columnCounter] != b[columnCounter]) 
                    {
                        if (sortingStrategies[j] == "ASC")
                            return a[columnCounter] < b[columnCounter];
                        else
                            return a[columnCounter] > b[columnCounter];
                    }
                    j++;
                }
                return false;
            });
            bufferManager.writePage(originalName, pageCounter, rowsInPage, rowsInPage.size());
            run.push_back(rowsInPage);
        }

        if (run.size() > 1)
        {
            // Merge the run
            vector<int> mergePositions(BLOCK_COUNT, 0);
            vector<vector<int>> mergedRows;
            int pageCounter = 0;
            for (int k = 0; k < BLOCK_COUNT*run[0].size(); k++)
            {
                bool exit_loop = true; // Exit when all merge positions are at the end of the run
                for (int j = 0; j < BLOCK_COUNT; j++) 
                {
                    if (mergePositions[j] < run[j].size()) 
                    {
                        exit_loop = false;
                        break;
                    }
                }
                if (exit_loop)
                    break;
                
                vector<pair<vector<int>, int>> compare; // Collection of rows from across all pages in the current run.
                for (int j = 0; j < BLOCK_COUNT; j++) 
                {
                    if (mergePositions[j] < run[j].size())
                        compare.push_back({run[j][mergePositions[j]], j});
                }

                int minIndex = comparator(columnIndexes, sortingStrategies, compare);
                mergedRows.push_back(run[minIndex][mergePositions[minIndex]]);
                if (mergedRows.size() == this->maxRowsPerBlock) 
                {
                    bufferManager.writePage(tempName, pageCounter, mergedRows, mergedRows.size());
                    pageCounter++;
                    mergedRows.clear();
                }
                mergePositions[minIndex]++;
            }
        }
    }

    // For table or part of table with pages < BLOCK_COUNT
    if (this->blockCount % BLOCK_COUNT != 0) 
    {
        // Sort remaining pages (< BLOCK_COUNT number of pages)
        vector<vector<vector<int>>> run;
        for (int pageCounter = numRuns*BLOCK_COUNT; pageCounter < this->blockCount; pageCounter++) 
        {
            vector<vector<int>> rowsInPage = bufferManager.getPage(originalName, pageCounter).getRows();
            sort(rowsInPage.begin(), rowsInPage.end(), [columnIndexes, sortingStrategies](vector<int> &a, vector<int> &b) {
                int i = 0;
                for (int columnCounter : columnIndexes) 
                {
                    if (a[columnCounter] != b[columnCounter]) 
                    {
                        if (sortingStrategies[i] == "ASC")
                            return a[columnCounter] < b[columnCounter];
                        else
                            return a[columnCounter] > b[columnCounter];
                    }
                    i++;
                }
                return false;
            });
            bufferManager.writePage(originalName, pageCounter, rowsInPage, rowsInPage.size());
            run.push_back(rowsInPage);
        }
        
        if (run.size() > 1)
        {
            // Merge the run
            vector<int> mergePositions(run.size(), 0);
            vector<vector<int>> mergedRows;
            int pageCounter = numRuns*BLOCK_COUNT;

            int limit = this->rowCount - numRuns*BLOCK_COUNT*this->maxRowsPerBlock;
            for (int k = 0; k < limit; k++) 
            {
                bool exit_loop = true;
                for (int j = 0; j < run.size(); j++) 
                {
                    if (mergePositions[j] < run[j].size()) 
                    {
                        exit_loop = false;
                        break;
                    }
                }
                if (exit_loop)
                    break;
                
                vector<pair<vector<int>, int>> compare;
                for (int j = 0; j < run.size(); j++) 
                {
                    if (mergePositions[j] < run[j].size())
                        compare.push_back({run[j][mergePositions[j]], j});
                }

                int minIndex = comparator(columnIndexes, sortingStrategies, compare);
                mergedRows.push_back(run[minIndex][mergePositions[minIndex]]);
                if (mergedRows.size() == this->maxRowsPerBlock) 
                {
                    bufferManager.writePage(tempName, pageCounter, mergedRows, mergedRows.size());
                    pageCounter++;
                    mergedRows.clear();
                }

                mergePositions[minIndex]++;
            }

            if (mergedRows.size() > 0) 
            {
                bufferManager.writePage(tempName, pageCounter, mergedRows, mergedRows.size());
                pageCounter++;
                mergedRows.clear();
            }
        }
    }

    // Delete the old pages
    if (this->blockCount > 1)
    {
        for (int i = 0; i < this->blockCount; i++) {
            bufferManager.deleteFile(originalName, i);
        }
        bufferManager.renamePages(tempName, originalName);
        bufferManager.flushPool();
    }

    // Sort internally within pages. Store them in run. Then go by mergepositions. Resize to suit the number of pages in one run
    // Each merge position is set to 0. Compare, reorder and iterate the required positions.

    // Outmost loop runs for log(n)/log(BLOCK_COUNT) times (n = number of pages)
    int x = (this->blockCount % BLOCK_COUNT == 0) ? 1 : 2; 
    int numLevels = ceil(log(this->blockCount)/log(BLOCK_COUNT)) - x;
    int offset = BLOCK_COUNT;
    string readPageName = originalName;
    string writePageName = tempName;
    int level = 0;
    
    for (level = 1; level <= numLevels; level++) 
    {
        int num_streams = this->blockCount/offset;

        vector<int> mergePositions(num_streams, 0);
        vector<vector<int>> mergedRows;
        int pageCounter = 0;

        Page pages[num_streams];
        for (int j = 0; j < num_streams; j++) 
        {
            pages[j] = bufferManager.getPage(readPageName, j*offset);
        }

        for (int k = 0; k < num_streams*offset*this->maxRowsPerBlock; k++) 
        {
            vector<pair<vector<int>, int>> compare;
            for (int j = 0; j < num_streams; j++) 
            {
                if (mergePositions[j] < offset*this->maxRowsPerBlock)
                    compare.push_back({pages[j].getRow(mergePositions[j]%this->maxRowsPerBlock), j});
            }
            vector<pair<vector<int>, int>> compare_original = compare;
            int minIndex = comparator(columnIndexes, sortingStrategies, compare);
            mergedRows.push_back(compare_original[minIndex].first);
            if (mergedRows.size() == this->maxRowsPerBlock) 
            {
                bufferManager.writePage(writePageName, pageCounter, mergedRows, mergedRows.size());
                pageCounter++;
                mergedRows.clear();
            }
            
            mergePositions[minIndex]++;
            if (mergePositions[minIndex] % this->maxRowsPerBlock == 0)
                pages[minIndex] = bufferManager.getPage(readPageName, minIndex*offset + mergePositions[minIndex]/this->maxRowsPerBlock);
        }

        offset *= BLOCK_COUNT;

        // Delete the old pages
        for (int i = 0; i < this->blockCount; i++) {
            bufferManager.deleteFile(readPageName, i);
        }
        bufferManager.renamePages(writePageName, readPageName);
        bufferManager.flushPool();
    }
    
    if (this->blockCount % BLOCK_COUNT != 0 && this->blockCount > 1)
    {
        int num_streams = 2;

        vector<int> mergePositions(num_streams, 0);
        vector<vector<int>> mergedRows;
        int pageCounter = 0;

        Page pages[num_streams];
        for (int j = 0; j < num_streams; j++) 
        {
            pages[j] = bufferManager.getPage(readPageName, j*offset);
        }

        for (int k = 0; k < this->rowCount; k++) 
        {
            vector<pair<vector<int>, int>> compare;
            if (mergePositions[0] < offset*this->maxRowsPerBlock) {
                compare.push_back({pages[0].getRow(mergePositions[0]%this->maxRowsPerBlock), 0});
            }
            if (mergePositions[1] < this->rowCount - offset*this->maxRowsPerBlock) {
                compare.push_back({pages[1].getRow(mergePositions[1]%this->maxRowsPerBlock), 1});
            }
            
            vector<pair<vector<int>, int>> compare_original = compare;
            
            if (mergePositions[0] >= offset*this->maxRowsPerBlock && mergePositions[1] >= this->rowCount - offset*this->maxRowsPerBlock)
                break;
            
            int minIndex = comparator(columnIndexes, sortingStrategies, compare);
            mergedRows.push_back(compare_original[minIndex].first);
            if (mergedRows.size() == this->maxRowsPerBlock) 
            {
                bufferManager.writePage(writePageName, pageCounter, mergedRows, mergedRows.size());
                pageCounter++;
                mergedRows.clear();
            }
            
            mergePositions[minIndex]++;
            if (mergePositions[minIndex] % this->maxRowsPerBlock == 0)
                pages[minIndex] = bufferManager.getPage(readPageName, minIndex*offset + mergePositions[minIndex]/this->maxRowsPerBlock);
        }
        if (mergedRows.size() > 0) 
        {
            bufferManager.writePage(writePageName, pageCounter, mergedRows, mergedRows.size());
            pageCounter++;
            mergedRows.clear();
        }

        // Delete the old pages
        for (int i = 0; i < this->blockCount; i++) {
            bufferManager.deleteFile(readPageName, i);
        }
        bufferManager.renamePages(writePageName, readPageName);
        bufferManager.flushPool();
    }
    bufferManager.flushPool();
}

/**
 * @brief Function that compares values in rows based on binary operator
 * 
 * @param value1
 * @param value2 
 * @param binaryOperator 
 */
bool Table::compare(int value1, int value2, string binaryOperator)
{
    if (binaryOperator == "<")
        return value1 < value2;
    else if (binaryOperator == "<=")
        return value1 <= value2;
    else if (binaryOperator == ">")
        return value1 > value2;
    else if (binaryOperator == ">=")
        return value1 >= value2;
    else if (binaryOperator == "==")
        return value1 == value2;
    else if (binaryOperator == "!=")
        return value1 != value2;
    else
        return false;
}

/**
 * @brief Function that groups the table by a column
 * 
 * @param aggFunction1
 * @param aggColumn1
 * @param binaryOperator
 * @param condValue
 * @param aggFunction2
 * @param aggColumn2
 * @param resultColumn
 * @param resultTable
 */
void Table::group(string groupColumn, string aggFunction1, string aggColumn1, string binaryOperator, int condValue, string aggFunction2, string aggColumn2, string resultColumn, Table* resultTable)
{
    logger.log("Table::group");

    // Get the column indexes
    int aggColumn1Index = this->getColumnIndex(aggColumn1);
    int aggColumn2Index = this->getColumnIndex(aggColumn2);
    // int resultColumnIndex = this->getColumnIndex(resultColumn);
    int groupColumnIndex = this->getColumnIndex(groupColumn);

    vector<int> distinctValues;
    for (auto value : this->distinctValuesInColumns[groupColumnIndex])
        distinctValues.emplace_back(value);
    sort(distinctValues.begin(), distinctValues.end());
    int pos = 0;

    // Get the cursor
    Cursor cursor(this->tableName, 0);
    // vector<int> aggColumn1Values;
    // vector<int> aggColumn2Values;
    // vector<int> resultColumnValues;
    // int prevAggColumn1Value = INT_MIN;
    // int prevAggColumn2Value = INT_MIN;
    // int resultColumnValue = 0;
    // int count = 0;
    int rowCount = 0;
    int pageCounter = 0;
    // int blockCounter = 0;

    unordered_map<string, int> aggFuncValues1 = {{"SUM", 0}, {"AVG", 0}, {"MIN", INT32_MAX}, {"MAX", INT32_MIN}, {"COUNT", 0}};
    unordered_map<string, int> aggFuncValues2 = {{"SUM", 0}, {"AVG", 0}, {"MIN", INT32_MAX}, {"MAX", INT32_MIN}, {"COUNT", 0}};

    vector<int> row;
    vector<vector<int>> newRows;
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++) 
    {
        row = cursor.getNext();
        
        if (row[groupColumnIndex] != distinctValues[pos]) 
        {
            // aggFuncValues1["AVG"] = aggFuncValues1["SUM"] / aggFuncValues1["COUNT"];
            // aggFuncValues2["AVG"] = aggFuncValues2["SUM"] / aggFuncValues2["COUNT"];
            if (this->compare(aggFuncValues1[aggFunction1], condValue, binaryOperator)) 
            {
                newRows.push_back({distinctValues[pos], aggFuncValues2[aggFunction2]});
                rowCount++;
                if (newRows.size() == resultTable->maxRowsPerBlock) 
                {
                    bufferManager.writePage(resultTable->tableName, pageCounter, newRows, newRows.size());
                    pageCounter++;
                    resultTable->rowsPerBlockCount.emplace_back(newRows.size());
                    newRows.clear();
                }
            }
            aggFuncValues1 = {{"SUM", 0}, {"AVG", 0}, {"MIN", INT32_MAX}, {"MAX", INT32_MIN}, {"COUNT", 0}};
            aggFuncValues2 = {{"SUM", 0}, {"AVG", 0}, {"MIN", INT32_MAX}, {"MAX", INT32_MIN}, {"COUNT", 0}};
            pos++;
        }

        aggFuncValues1["SUM"] = aggFuncValues1["SUM"] + row[aggColumn1Index];
        aggFuncValues1["COUNT"] = aggFuncValues1["COUNT"] + 1;
        aggFuncValues1["AVG"] = aggFuncValues1["SUM"] / aggFuncValues1["COUNT"];
        aggFuncValues1["MIN"] = min(aggFuncValues1["MIN"], row[aggColumn1Index]);
        aggFuncValues1["MAX"] = max(aggFuncValues1["MAX"], row[aggColumn1Index]);
        aggFuncValues2["SUM"] = aggFuncValues2["SUM"] + row[aggColumn2Index];
        aggFuncValues2["COUNT"] = aggFuncValues2["COUNT"] + 1;
        aggFuncValues2["AVG"] = aggFuncValues2["SUM"] / aggFuncValues2["COUNT"];
        aggFuncValues2["MIN"] = min(aggFuncValues2["MIN"], row[aggColumn2Index]);
        aggFuncValues2["MAX"] = max(aggFuncValues2["MAX"], row[aggColumn2Index]);
    }

    if (newRows.size() > 0)
    {
        bufferManager.writePage(resultTable->tableName, pageCounter, newRows, newRows.size());
        pageCounter++;
        resultTable->rowsPerBlockCount.emplace_back(newRows.size());
        newRows.clear();
    }

    resultTable->rowCount = rowCount;
    resultTable->blockCount = pageCounter;
    bufferManager.flushPool();
}
