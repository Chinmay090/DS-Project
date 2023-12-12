#include "global.h"

void TableCatalogue::insertTable(Table* table)
{
    logger.log("TableCatalogue::~insertTable"); 
    this->tables[table->tableName] = table;
}
void TableCatalogue::deleteTable(string tableName)
{
    logger.log("TableCatalogue::deleteTable"); 
    this->tables[tableName]->unload();
    delete this->tables[tableName];
    this->tables.erase(tableName);
}
Table* TableCatalogue::getTable(string tableName)
{
    logger.log("TableCatalogue::getTable"); 
    Table *table = this->tables[tableName];
    return table;
}
bool TableCatalogue::isTable(string tableName)
{
    logger.log("TableCatalogue::isTable"); 
    if (this->tables.count(tableName))
        return true;
    return false;
}

bool TableCatalogue::isColumnFromTable(string columnName, string tableName)
{
    logger.log("TableCatalogue::isColumnFromTable"); 
    if (this->isTable(tableName))
    {
        Table* table = this->getTable(tableName);
        if (table->isColumn(columnName))
            return true;
    }
    return false;
}

bool TableCatalogue::isMatrix(string matrixName)
{
    logger.log("TableCatalogue::isMatrix");
    if(this->tables.count(matrixName) && this->tables[matrixName]->isMatrix)
        return true;
    return false;
}

void TableCatalogue::print()
{
    logger.log("TableCatalogue::print"); 
    cout << "\nRELATIONS" << endl;

    int rowCount = 0;
    for (auto rel : this->tables)
    {
        cout << rel.first << endl;
        rowCount++;
    }
    printRowCount(rowCount);
}

void TableCatalogue::renameMatrix(string fromMatrixName, string toMatrixName) 
{
    logger.log("TableCatalogue::renameMatrix");
    auto p = this->tables.find(fromMatrixName);
    this->tables[toMatrixName] = p->second;
    this->tables[toMatrixName]->tableName = toMatrixName;
    this->tables.erase(fromMatrixName);
    bufferManager.renamePages(fromMatrixName, toMatrixName);
    this->tables[toMatrixName]->sourceFileName = "../data/temp/" + toMatrixName + ".csv";
}

TableCatalogue::~TableCatalogue(){
    logger.log("TableCatalogue::~TableCatalogue"); 
    for(auto table: this->tables){
        table.second->unload();
        delete table.second;
    }
}
