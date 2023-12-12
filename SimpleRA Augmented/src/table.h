#include "cursor.h"

enum IndexingStrategy
{
    BTREE,
    HASH,
    NOTHING
};

/**
 * @brief The Table class holds all information related to a loaded table. It
 * also implements methods that interact with the parsers, executors, cursors
 * and the buffer manager. There are typically 2 ways a table object gets
 * created through the course of the workflow - the first is by using the LOAD
 * command and the second is to use assignment statements (SELECT, PROJECT,
 * JOIN, SORT, CROSS and DISTINCT). 
 *
 */
class Table
{
    vector<unordered_set<int>> distinctValuesInColumns;

public:
    string sourceFileName = "";
    string tableName = "";
    vector<string> columns;
    vector<uint> distinctValuesPerColumnCount;
    uint columnCount = 0;
    long long int rowCount = 0;
    uint blockCount = 0;
    uint maxRowsPerBlock = 0;
    uint maxColumnsPerBlock = 0;
    vector<uint> rowsPerBlockCount;
    vector<uint> columnsPerBlockCount;
    bool indexed = false;
    string indexedColumn = "";
    IndexingStrategy indexingStrategy = NOTHING;
    bool isMatrix = false;
    int blocksPerRow = 1;
    
    bool extractColumnNames(string firstLine);
    bool extractMatrixParams(string firstLine);
    bool blockify();
    bool blockifyMatrix();
    void updateStatistics(vector<int> row);
    Table();
    Table(string tableName);
    Table(string tableName, vector<string> columns);
    Table(Table *temp);
    bool load();
    bool isColumn(string columnName);
    void renameColumn(string fromColumnName, string toColumnName);
    void print();
    void makePermanent();
    bool isPermanent();
    void getNextPage(Cursor *cursor);
    Cursor getCursor();
    int getColumnIndex(string columnName);
    void unload();
    void sortMerge(vector<int> columnIndexes, vector<string> sortingStrategies, string tempName, string originalName);
    int comparator(vector<int> &columnIndexes, vector<string> &sortingStrategies, vector<pair<vector<int>, int>> &rowPairs);
    void group(string groupColumn, string aggFunction1, string aggColumn1, string binaryOperator, int condValue, string aggFunction2, string aggColumn2, string resultColumn, Table* resultTable);
    bool compare(int value1, int value2, string binaryOperator);

/**
 * @brief Static function that takes a vector of valued and prints them out in a
 * comma seperated format.
 *
 * @tparam T current usaages include int and string
 * @param row 
 */
template <typename T>
void writeRow(vector<T> row, ostream &fout)
{
    logger.log("Table::printRow");
    for (int columnCounter = 0; columnCounter < row.size(); columnCounter++)
    {
        if (columnCounter != 0)
            fout << ", ";
        fout << row[columnCounter];
    }
    if (!this->isMatrix)
        fout << endl;
}

/**
 * @brief Static function that takes a vector of valued and prints them out in a
 * comma seperated format.
 *
 * @tparam T current usaages include int and string
 * @param row 
 */
template <typename T>
void writeRow(vector<T> row)
{
    logger.log("Table::printRow");
    ofstream fout(this->sourceFileName, ios::app);
    this->writeRow(row, fout);
    fout.close();
}
};