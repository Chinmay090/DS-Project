# Data Systems - Project Phase 1

### Team 7
- Aneesh Chavan (2020111018)
- Vikram Rao (2020101123)
- Chinmay Deshpande (2020102069)

## Functions Implemented
1. EXTERNAL SORT
2. JOIN
3. ORDER BY
4. GROUP BY
<hr>

### EXTERNAL SORT 
- This is done by using a sort-and-merge implementation of the k-way merge, merging at most 10 pages at a time. We split the process into runs consisting of 10 pages at a time. Before we talk about how the pages are sorted, we should talk about the priority order. Take the following query, for instance:

SORT EMPLOYEE BY GENDER, SALARY IN ASC, DESC 

- We have made the assumption that this means that we sort by gender first and within the gender sorted columns we then sort by salary. Effectively saying that if the gender is same then salary acts as the tiebreaker. This is the logic followed in our algorithms.

- We split the sorting into runs of 10 pages each. In each run, we first sort all 10 pages individually using the order stated above. Following that, we merge the runs. This is done by setting a position counter array which holds the row position in each page, along with an array of 10 temporary pages to which we write the final table in the correct order across pages. Initially, the entire array is set to 0. Then using this position counter we iterate through each row in each page and compare them, pushing the least value back into the corresponding temporary page 

- Let’s take the case of 3 pages.

Each page counter is set to 0 at first.
Then we look at the first row in each of the 3 pages and compare them. If the first row of the first page is the least, we write it back into the first temporary page and increment the first element of the position counter array, keeping the others the same. Then the comparison happens again, and this time, if the first row of the second page is the least it is written into the first temporary page and the position counter for the second page is incremented by 1. This compare and write process carries on until the first temporary page is filled, then we move to the next temporary page and repeat until all the temporary pages have been filled.

- After the temporary pages have been completely written, we flush the original pages and rename the temporary pages to have the same header as the original pages. The table is thus sorted.

### JOIN
- We efficiently join tables using sorting and indexing. By sorting the larger table on the column to be joined, and using a scheme similar to clustering indexes, we optimise our joins. After obtaining the required value from the first row in each block, we iterate through all rows in the first table, which has also been sorted on the column to be joined. This way, whenever a new value is encountered in the first table, we loop through the stored indices and determine which blocks need to be loaded and searched based on the user-supplied comparator. This greatly minimises the number loads that are made and increases the speed of the algorithm. The resulting table is stored as blocks but not as a .csv, so it needs to be exported.

General strategy:

1. sort both tables based on the respective columns

2. create a new empty table for the result, keep adding rows to it page by page

3. get indexes for second table for each page

4. iterate through all first table rows

5. when a unique value for the 1st row column is found, scan the second table for all pages that 
potentially have values to join. keep this list of pages

5.5. change rules to determine which blocks are to be used based on comparator

6. iterate through all these pages, add rows as necessary

7. keep going through first table rows, if a new value is seen, get a new set of blocks

### ORDER BY 
- This command requires the creation of a new table with the rows sorted by a particular attribute. A new table is created as a copy of the old table, with a different table name as specified by the user in the input command. In this case, let's call it Result.  And the external sorting function described above is applied on table Result. This means that we do the in place sorting on Result to directly create the final output table.

(We will use Result to denote the output table name for group by and join as well)

### GROUP BY 
- This command also requires the creation of a new table, this time consisting of all rows satisfying a particular mathematical condition (aggregate condition).  The external sort function is applied on a temporary copy of the table in memory so that the original table remains unaffected. The temporary table is deleted after the group by function. The main group by function works by iterating through the entire table and keeping track of all aggregate statistics of both the condition column as well as the result column specified in the command, by means of 2 unordered maps. These maps are cleared after each distinct entry in the grouping column. This is done to ensure that only the required number of rows (one per distinct entry of the grouping column) is written to the pages of the final result table. 
