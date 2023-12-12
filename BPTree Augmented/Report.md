Data Systems - Project Phase 3

Team 7

Aneesh Chavan (2020111018)
Vikram Rao (2020101123)
Chinmay Deshpande (2020102069)

Functions Implemented - 
INSERT
DELETE

INSERT - We insert a new node when the current node’s size exceeds FANOUT. Here, there are 2 types of inserts - one for leaf nodes and one for internal nodes. 
The leaf node case is simple - we simply insert the <key, RecordPtr> pair into the map (which is already sorted). If the leaf node overflows (size > FANOUT), then we create a new leaf node and transfer some data pointers from the current leaf node to the new one so that both leaf nodes have at least MIN_OCCUPANCY number of data pointers. If overflow occurs, we return the pointer to the new leaf, else we return NULL_PTR. 
For internal node insert, we follow a recursive approach, where we recursively call the insert_key function at the appropriate child node, by finding the correct position among and internal node’s tree pointers. While returning up the tree (back to root node) from the recursion, either the child node we called insert_key on is a leaf node or internal node. 
If the return value is NULL_PTR for either case, we just return NULL_PTR, since no more splits are needed. 
Also, in both cases, if the current internal node’s size is less than FANOUT, it means there is space in the current internal node, and the splitting stops at the child node (whether the child node is leaf node or internal node). In this case, we just add the new split node’s pointer to the current internal node’s list of tree pointers and update the keys accordingly. After this, we return NULL_PTR, since no more splits are needed.
The final case is when the child node splits, and we need to accommodate another tree pointer in the current internal node, but the current internal node will overflow. In this case, we create a new internal node and transfer the tree pointers and keys between the current node and the new internal node, such that the MIN_OCCUPANCY rule is followed for both nodes. The pointer of the newly created internal node is returned.


DELETE - Like INSERT, this has two cases as well. The base case is for leaf nodes, where we remove the key from the leaf node.
For the internal node, we use a recursive function. Starting at the root, we move down through the layers until we reach the leaf node. After that, as the recursion control flow goes back up the tree, we check if the child node is a leaf node or an internal node, as well as checking for underflow in both cases. If there is an underflow in either case, then we check the siblings in priority order to see if they have any extra keys to redistribute. The condition applied here is that the sum of the sizes of the current child node and any one of the siblings going by priority, should be greater than 2*MIN_OCCUPANCY. If this happens, then we can rearrange the keys. If this does not happen, then the nodes need to be merged.
At every stage, we must ensure that the order of keys is maintained after every operation, and we use the child_node->max() function to keep tabs on this.


RANGE - The RANGE statement gives us the number of block accesses by use of the B+ tree as well as without the B+ tree, with the unordered heap. As seen in the following graphs, we can see that for each of the 100 queries, we have a variable number of block accesses by the B+ tree, but a constant number of accesses without it. However, we can see that the average number of block accesses is significantly less for the B+ tree. The reason for this is that the use of an index in a B+ tree enforces a kind of binary search-like structure to the block searches, while the unordered heap is just linear searching.
The only exception to this is if the FANOUT value is too high, in which case this brings a multitude of nodes on the same level, thereby forcing another linear search, which brings the average number of block searches close to that without the B+ tree. This can be seen in the case where FANOUT is set to 50.
