#include"semanticParser.h"

void executeCommand();

void executeCHECKSYMMETRY();
void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeINDEX();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executePRINT();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();
void executeTRANSPOSE();
void executeCOMPUTE();
void executeORDER();
void executeGROUP();

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);