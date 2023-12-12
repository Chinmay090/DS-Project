# Data Systems - Project Phase 1

### Team 7
- Aneesh Chavan (2020111018)
- Vikram Rao (2020101123)
- Chinmay Deshpande (2020102069)

## Functions Implemented
1. LOAD MATRIX
2. PRINT MATRIX
3. EXPORT MATRIX
4. RENAME MATRIX
5. TRANSPOSE
6. CHECKSYMMETRY
7. COMPUTE
<hr>

### LOAD MATRIX <matrix_name>
- The Load Matrix command reads the file matrix_name.csv and stores it as a matrix if the file exists. 
- The original code reads the first line and assumes that that line contains the column names, which is not the case for matrices. So for this we added a flag to the table definition that tells us if a matrix is being read. The flag is called isMatrix. If the first line of the input file contains only numbers, then we know that the given file is a matrix and isMatrix is set to true. 
- We also added another function, extractMatrixParams, to obtain the number of rows and columns in the matrix. We also calculate the maximum rows per block (or page), which is the same as maxColumnsPerBlock, since we are storing submatrices of a fixed size in each page and the original matrix is square. We also calculate the number of blocks required to store a row (or column) which helps organize the data stored in each page in the blockifyMatrix function.
- Each block can accommodate 250 4-byte integers when the block size is 1000 bytes, so the number of blocks accessed will depend on the number of elements in the matrix.
- When blockifyMatrix is called, it splits the matrix into pages such that each page (or block) can store a maximum of maxRowsPerBlock x maxColumnPerBlock elements. This essentially means that row-wise submatrices of the original matrix are stored in sequential pages. So, if the matrix dimension is 16 X 16 and the block size is 1000, the first 15 x 15 submatrix will be stored in the first page, the remaining 15 x 1 elements will be stored in the second page, the 1 x 15 elements in the last row will be stored in the third page and the last 1 x 1 element will be stored in the fourth page. It is like a row-wise ordering of blocks containing submatrices of a fixed maximum size.
- We don't change the deque of BufferManager, so we retain the page naming convention and store the pages sequentially as before. 
<hr>

### PRINT MATRIX <matrix_name>
- For print, we added a separate section for matrices in Table::print function. Since all columns of the matrix need not be in the same page, we need to use a nested loop to print each row of the appropriate page in memory.
- For this, we need to compute the correct page index to pass to the Cursor object, along with the appropriate row offset within the page, to access the correct row before printing it. This required adding a new constructor in the Cursor class and modifying the page constructor to restrict the number of columns read into a vector when BufferManager fetches the page for the Cursor.
- We also needed to modify the writeRow function a little, to format the printing for matrices.
<hr>

### EXPORT MATRIX <matrix_name>
- Similar to the PRINT MATRIX command, we created a separate section for matrices in the Table::makePermanent function, except instead of writing out each row of the matrix to the cout buffer, we write out to the file stream, where the file name is given by the sourceFileName variable. 
<hr>

### RENAME MATRIX <matrix_currentname> <matrix_newname>
- Rename Matrix renames a matrix that is already loaded in memory. We accomplish this by adding a new table catalogue entry with the new matrix name and removing its old entry. 
- We also rename all pages used to contain the new matrix name. This is done by renaming all the pages in the <i>temp</i> folder that correspond to the loaded matrix. Here, we use the filestream library to iterate through the <i>temp</i> directory. 
- The source file path is also changed to have the new table name (so that EXPORT MATRIX will work as expected).
<hr>

### TRANSPOSE MATRIX <matrix_name>
- Transpose requires the use of two pages - the query page and the target page. 
- In a nutshell, we go through the elements of the query page to swap. If necessary, we load new blocks if we need to access an element from a separate page. We do the same for the target page.
- Then we update all blocks stored in the main memory to reflect the changes made.
- Do the final write to pages.
<hr>

### CHECKSYMMETRY <matrix_name>
- We check whether the matrix is symmetrical or not by using vector of vectors to store the data read from a page. 
- If the matrix is small enough to fit into a single block when loaded, then only 1 block is accessed and the 2D vector of vectors is used to check the symmetry of the matrix. For larger matrices, to avoid overflow, 2 two-dimensional vectors are used and are cleared when a new block needs to be accessed. 
- This is done to handle cases when an element of the matrix at a particular position or index is in a separate page from the corresponding entry of the transposed matrix, but they still need to be checked for equality. 
<hr>

### COMPUTE <matrix_name>
- Effectively the same as transpose.
- Access is done similarly, but we compute for each element of the matrix as we go.
