#include "tableCatalogue.h"

using namespace std;

enum QueryType
{
    CHECKSYMMETRY,
    CLEAR,
    COMPUTE,
    CROSS,
    DISTINCT,
    EXPORT,
    GROUP,
    INDEX,
    JOIN,
    LIST,
    LOAD,
    ORDER,
    PRINT,
    PROJECTION,
    RENAME,
    SELECTION,
    SORT,
    SOURCE,
    TRANSPOSE,
    UNDETERMINED
};

enum BinaryOperator
{
    LESS_THAN,
    GREATER_THAN,
    LEQ,
    GEQ,
    EQUAL,
    NOT_EQUAL,
    NO_BINOP_CLAUSE
};

enum SortingStrategy
{
    ASC,
    DESC,
    NO_SORT_CLAUSE
};

enum SelectType
{
    COLUMN,
    INT_LITERAL,
    NO_SELECT_CLAUSE
};

class ParsedQuery
{

public:
    QueryType queryType = UNDETERMINED;

    string checksymmetrymatrixName = "";

    string clearRelationName = "";

    string computematrixName = "";

    string crossResultRelationName = "";
    string crossFirstRelationName = "";
    string crossSecondRelationName = "";

    string distinctResultRelationName = "";
    string distinctRelationName = "";

    string exportRelationName = "";

    string groupColumn = "";
    string groupResultRelationName = "";
    string groupSourceRelationName = "";
    string groupAggFunction1 = "";
    string groupAggColumn1 = "";
    string groupBinaryOperator = "";
    int groupCondValue = 0;
    string groupAggFunction2 = "";
    string groupAggColumn2 = "";
    string groupResultColumn = "";

    IndexingStrategy indexingStrategy = NOTHING;
    string indexColumnName = "";
    string indexRelationName = "";

    BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
    string joinResultRelationName = "";
    string joinFirstRelationName = "";
    string joinSecondRelationName = "";
    string joinFirstColumnName = "";
    string joinSecondColumnName = "";

    string loadRelationName = "";

    string printRelationName = "";

    string projectionResultRelationName = "";
    vector<string> projectionColumnList;
    string projectionRelationName = "";

    string renameFromColumnName = "";
    string renameToColumnName = "";
    string renameRelationName = "";

    string renameFromMatrixName = "";
    string renameToMatrixName = "";

    SelectType selectType = NO_SELECT_CLAUSE;
    BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
    string selectionResultRelationName = "";
    string selectionRelationName = "";
    string selectionFirstColumnName = "";
    string selectionSecondColumnName = "";
    int selectionIntLiteral = 0;

    SortingStrategy sortingStrategy = NO_SORT_CLAUSE;
    string sortResultRelationName = "";
    string sortColumnName = "";
    string sortRelationName = "";
    
    vector<string> sortingStrategies{};
    vector<string> sortColumnNames{};
    vector<int> sortColumnIdxs{};

    string sourceFileName = "";

    string transposeMatrixName = "";

    ParsedQuery();
    void clear();
};

bool syntacticParse();
bool syntacticParseCHECKSYMMETRY();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseEXPORT();
bool syntacticParseINDEX();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParsePRINT();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseSELECTION();
bool syntacticParseSORT();
bool syntacticParseSOURCE();
bool syntacticParseTRANSPOSE();
bool syntacticParseCOMPUTE();
bool syntacticParseORDER();
bool syntacticParseGROUP();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);
