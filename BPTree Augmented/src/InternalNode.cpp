 #include "InternalNode.hpp"
 #include "LeafNode.hpp"
 #include "RecordPtr.hpp"

//creates internal node pointed to by tree_ptr or creates a new one
InternalNode::InternalNode(const TreePtr &tree_ptr) : TreeNode(INTERNAL, tree_ptr) {
    this->keys.clear();
    this->tree_pointers.clear();
    if (!is_null(tree_ptr))
        this->load();
}

//max element from tree rooted at this node
Key InternalNode::max() {
    Key max_key = DELETE_MARKER;
    TreeNode* last_tree_node = TreeNode::tree_node_factory(this->tree_pointers[this->size - 1]);
    max_key = last_tree_node->max();
    delete last_tree_node;
    return max_key;
}

//if internal node contains a single child, it is returned
TreePtr InternalNode::single_child_ptr() {
    if (this->size == 1)
        return this->tree_pointers[0];
    return NULL_PTR;
}

/*inserts <key, record_ptr> into subtree rooted at this node.
returns pointer to split node if exists

recursively called to handle splits that need need to be propagated up the tree


*/
//TODO: InternalNode::insert_key to be implemented
TreePtr InternalNode::insert_key(const Key &key, const RecordPtr &record_ptr) {
    TreePtr new_tree_ptr = NULL_PTR;
    // cout << "InternalNode::insert_key not implemented" << endl;

    // determine which "branch" we go down
    int idx = 0;    // branch K_0  (less than all)
    if (key > this->keys[this->keys.size()-1]) {
        idx = this->keys.size();        // branch K_p (greater than all)
    } 
    // search for an intermediate branch
    else if (key > this->keys[0]) {
        for (int i = 0; i < this->keys.size()-1; i++) {
            if (this->keys[i] < key && key <= this->keys[i+1]) {
                idx = i+1;
                break;
            }
        }
    }
    
    // make substitute, make the opbject (not actually dumping it)
    auto child_node = TreeNode::tree_node_factory(this->tree_pointers[idx]);
    TreePtr potential_split_node_ptr = child_node->insert_key(key, record_ptr);     // recurse, eventually reaches LeafNode::insert_key
    
    // checks for a returned null, this means that no splitting occured
    // delete the TreeNode object, return new_tree_ptr which is still null
    if (is_null(potential_split_node_ptr)) {
        delete child_node;
        return new_tree_ptr;
    }

    // splitting HAS occured, NO MORE SPLITTING, insert the new node and modify keys, sizes
    if (this->size < FANOUT) {
        this->tree_pointers.insert(this->tree_pointers.begin() + idx + 1, potential_split_node_ptr);   // add the newly created node in the right place
        this->keys.insert(this->keys.begin() + idx, child_node->max());
        this->size++;
        this->dump();
        delete child_node;
        return new_tree_ptr;
    }

    // splitting HAS occurred, MORE SPLITS NEEDED
    this->tree_pointers.insert(this->tree_pointers.begin() + idx + 1, potential_split_node_ptr);
    this->keys.insert(this->keys.begin() + idx, child_node->max());
    this->size++;
    
    // internal node overflowed, generate a
    InternalNode *new_node = (InternalNode *) TreeNode::tree_node_factory(INTERNAL);
    int j = 0;
    for (auto it = this->tree_pointers.begin(); it != this->tree_pointers.end(); it++) {
        if (j < MIN_OCCUPANCY) {
        }
        else{
            // add to new node
            new_node->tree_pointers.push_back(this->tree_pointers[j]);
            auto temp_node = TreeNode::tree_node_factory(this->tree_pointers[j]);
            if(j != this->tree_pointers.size() - 1)
                new_node->keys.push_back(temp_node->max());                     // make sure theres always one more ptr than key value
            delete temp_node;
            
            // del from old node
            // for(auto abc : this->tree_pointers)
            //     cout << abc << " ";
            // cout << endl;
            // for(auto abc : this->keys)
            //     cout << abc << " ";
            // cout << endl;
            // cout << endl;

            // this->tree_pointers.erase(it--);
            // this->keys.erase(this->keys.begin() + MIN_OCCUPANCY - 1);
        }
        j++;
    }

    // clean up hanging key in original node
    this->keys.erase(this->keys.begin() + MIN_OCCUPANCY  - 1);      // make sure theres always one more ptr than key value
    
    // bookkeeping
    new_node->size = this->size - MIN_OCCUPANCY;
    this->size = MIN_OCCUPANCY;
    new_node->dump();
    new_tree_ptr = new_node->tree_ptr;
    delete child_node;
    delete new_node;
    this->dump();
    return new_tree_ptr;
}

//deletes key from subtree rooted at this if exists
//TODO: InternalNode::delete_key to be implemented
void InternalNode::delete_key(const Key &key) {
    TreePtr new_tree_ptr = NULL_PTR;
    // find correct branch to go down
    int idx = 0;        // branch K_0 (default)
    if (key > this->keys[this->keys.size()-1]) {
        idx = this->keys.size();                // branch K_p
    } 
    // search for intermediate branch
    else if (key > this->keys[0]) {
        for (int i = 0; i < this->keys.size()-1; i++) {
            if (this->keys[i] < key && key <= this->keys[i+1]) {
                idx = i+1;
                break;
            }
        }
    }

    // load chosen child key, call delete
    auto child_node = TreeNode::tree_node_factory(this->tree_pointers[idx]);
    child_node->delete_key(key);

    // no underflow
    if(child_node->size >= MIN_OCCUPANCY) {
        if(idx != this->keys.size()){
            this->keys[idx] = child_node->max();
            this->dump();
        }
        delete child_node;
        return;
    }

    /* 
        underflow occurs, check the order:
        1. if left exists, 
                check if redist possible
                else, merge
        2. if right exists, 
                check if redist possible
                else, merge
        
        there is no scenario where neither merging or redistribution is not possible
    */
    if (child_node->node_type == LEAF) {
        // LEAF NODE
        LeafNode* center_node = (LeafNode*) child_node;
        if(idx != 0){       
            // left exists
            auto left_node = (LeafNode*) TreeNode::tree_node_factory(this->tree_pointers[idx-1]);

            // no need to subtract 1, deletion and size update happens internally
            if (center_node->size + left_node->size >= 2 * MIN_OCCUPANCY){

                // redist is possible
                while(center_node->size < MIN_OCCUPANCY){
                    
                    // steal a record
                    auto itr = --left_node->data_pointers.end();
                    center_node->data_pointers.insert(make_pair(itr->first, itr->second));
                    left_node->data_pointers.erase(itr);

                    center_node->size++;
                    left_node->size--;
                }

                left_node->dump();
                center_node->dump();

                this->keys[idx-1] = left_node->max();

                delete left_node;
            }
            else{
                // merge nodes
                while(center_node->data_pointers.size() != 0){
                    auto itr = --center_node->data_pointers.end();

                    left_node->data_pointers.insert(make_pair(itr->first, itr->second));
                    center_node->data_pointers.erase(itr);

                    left_node->size++;
                    center_node->size--;
                }

                left_node->dump();
                center_node->dump();
                center_node->delete_node();
                this->keys[idx-1] = left_node->max();
                delete left_node;

                this->tree_pointers.erase(this->tree_pointers.begin() + idx);
                //this->keys.erase(this->keys.begin() + idx);
                this->size--;
            }
        }
        else if(idx != this->size - 1){
            // right exists
            auto right_node = (LeafNode*) TreeNode::tree_node_factory(this->tree_pointers[idx+1]);
            if (center_node->size + right_node->size >= 2 * MIN_OCCUPANCY){
                // redist is possible
                while(center_node->size < MIN_OCCUPANCY){
                    
                    // steal a record
                    auto itr = right_node->data_pointers.begin();
                    center_node->data_pointers.insert(make_pair(itr->first, itr->second));
                    right_node->data_pointers.erase(itr);

                    center_node->size++;
                    right_node->size--;
                }

                right_node->dump();
                center_node->dump();

                this->keys[idx] = center_node->max();

                delete right_node;
            }
            else{
                // merge nodes
                while(center_node->data_pointers.size() != 0){
                    auto itr = --center_node->data_pointers.end();

                    right_node->data_pointers.insert(make_pair(itr->first, itr->second));
                    center_node->data_pointers.erase(itr);

                    right_node->size++;
                    center_node->size--;
                }

                right_node->dump();
                center_node->dump();
                // this->keys[idx+1] = center_node->max();

                center_node->delete_node();
                delete right_node;

                this->tree_pointers.erase(this->tree_pointers.begin() + idx);
                this->keys.erase(this->keys.begin() + idx);
                this->size--;
            }
        }
        else assert(0);
    }
    // INTERNAL NODE
    else if(child_node->node_type == INTERNAL){
        InternalNode* center_node = (InternalNode*) child_node;
        if(idx != 0){       
            // left exists
            auto left_node = (InternalNode*) TreeNode::tree_node_factory(this->tree_pointers[idx-1]);

            // no need to subtract 1, deletion and size update happens internally
            if (center_node->size + left_node->size >= 2 * MIN_OCCUPANCY){

                // redist is possible
                while(center_node->size < MIN_OCCUPANCY){
                    
                    // steal a record
                    auto itr = --left_node->tree_pointers.end();
                    center_node->tree_pointers.insert(center_node->tree_pointers.begin(), *itr);
                    left_node->tree_pointers.erase(itr);

                    center_node->size++;
                    left_node->size--;
                }

                // make sure all keys are correct
                for(int k = 0; k < center_node->size - 1; k++){
                    auto temp = TreeNode::tree_node_factory(center_node->tree_pointers[k]);
                    if(temp->node_type == LEAF){
                        auto temp2 = (LeafNode*) temp;
                        center_node->keys[k] = temp2->max();
                    }
                    else{
                        auto temp2 = (InternalNode*) temp;
                        center_node->keys[k] = temp2->max();
                    }
                }

                for(int k = 0; k < left_node->size - 1; k++){
                    auto temp = TreeNode::tree_node_factory(left_node->tree_pointers[k]);
                    if(temp->node_type == LEAF){
                        auto temp2 = (LeafNode*) temp;
                        left_node->keys[k] = temp2->max();
                    }
                    else{
                        auto temp2 = (InternalNode*) temp;
                        left_node->keys[k] = temp2->max();
                    }
                }

                // dump
                left_node->dump();
                center_node->dump();

                this->keys[idx-1] = left_node->max();

                delete left_node;
            }
            else{
                // merge nodes
                for(int k = 0; k < center_node->tree_pointers.size(); k++){
                    left_node->tree_pointers.insert(left_node->tree_pointers.end(), center_node->tree_pointers[k]);
                    left_node->size++;
                }

                center_node->tree_pointers.clear();
                center_node->size = 0;
                center_node->delete_node();

                // make sure all keys are correct
                for(int k = 0; k < left_node->size - 1; k++){
                    auto temp = TreeNode::tree_node_factory(left_node->tree_pointers[k]);
                    if(temp->node_type == LEAF){
                        auto temp2 = (LeafNode*) temp;
                        left_node->keys[k] = temp2->max();
                    }
                    else{
                        auto temp2 = (InternalNode*) temp;
                        left_node->keys[k] = temp2->max();
                    }
                }

                // dump
                left_node->dump();
                this->keys[idx-1] = left_node->max();
                delete left_node;

                this->tree_pointers.erase(this->tree_pointers.begin() + idx);
                this->size--;
            }
        }
        else if(idx != this->size - 1){
            // right exists
            auto right_node = (InternalNode*) TreeNode::tree_node_factory(this->tree_pointers[idx+1]);
            if (center_node->size + right_node->size >= 2 * MIN_OCCUPANCY){
                // redist is possible
                while(center_node->size < MIN_OCCUPANCY){
                    
                    // steal a record
                    auto itr = right_node->tree_pointers.begin();
                    center_node->tree_pointers.insert(center_node->tree_pointers.end(), *itr);
                    right_node->tree_pointers.erase(itr);

                    center_node->size++;
                    right_node->size--;
                }

                // make sure all keys are correct
                for(int k = 0; k < center_node->size - 1; k++){
                    auto temp = TreeNode::tree_node_factory(center_node->tree_pointers[k]);
                    if(temp->node_type == LEAF){
                        auto temp2 = (LeafNode*) temp;
                        center_node->keys[k] = temp2->max();
                    }
                    else{
                        auto temp2 = (InternalNode*) temp;
                        center_node->keys[k] = temp2->max();
                    }
                }

                for(int k = 0; k < right_node->size - 1; k++){
                    auto temp = TreeNode::tree_node_factory(right_node->tree_pointers[k]);
                    if(temp->node_type == LEAF){
                        auto temp2 = (LeafNode*) temp;
                        right_node->keys[k] = temp2->max();
                    }
                    else{
                        auto temp2 = (InternalNode*) temp;
                        right_node->keys[k] = temp2->max();
                    }
                }

                right_node->dump();
                center_node->dump();

                this->keys[idx] = center_node->max();

                delete right_node;
            }
            else{
                // merge nodes
                for(int k = center_node->tree_pointers.size() - 1; k >= 0; k--){
                    right_node->tree_pointers.insert(right_node->tree_pointers.begin(), center_node->tree_pointers[k]);
                    right_node->size++;
                }

                center_node->tree_pointers.clear();
                center_node->size = 0;
                center_node->delete_node();

                // make sure all keys are correct
                for(int k = 0; k < right_node->size - 1; k++){
                    auto temp = TreeNode::tree_node_factory(right_node->tree_pointers[k]);
                    if(temp->node_type == LEAF){
                        auto temp2 = (LeafNode*) temp;
                        right_node->keys[k] = temp2->max();
                    }
                    else{
                        auto temp2 = (InternalNode*) temp;
                        right_node->keys[k] = temp2->max();
                    }
                }

                // dump
                right_node->dump();
                // this->keys[idx] = center_node->max();
                delete right_node;

                this->tree_pointers.erase(this->tree_pointers.begin() + idx);
                this->size--;
            }
        }
        else assert(0);

    }
    else assert(0);
    // Underflow for internal node
    this->dump();
}

//runs range query on subtree rooted at this node
void InternalNode::range(ostream &os, const Key &min_key, const Key &max_key) const {
    BLOCK_ACCESSES++;
    for (int i = 0; i < this->size - 1; i++) {
        if (min_key <= this->keys[i]) {
            auto* child_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
            child_node->range(os, min_key, max_key);
            delete child_node;
            return;
        }
    }
    auto* child_node = TreeNode::tree_node_factory(this->tree_pointers[this->size - 1]);
    child_node->range(os, min_key, max_key);
    delete child_node;
}

//exports node - used for grading
void InternalNode::export_node(ostream &os) {
    TreeNode::export_node(os);
    for (int i = 0; i < this->size - 1; i++)
        os << this->keys[i] << " ";
    os << endl;
    for (int i = 0; i < this->size; i++) {
        auto child_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
        child_node->export_node(os);
        delete child_node;
    }
}

//writes subtree rooted at this node as a mermaid chart
void InternalNode::chart(ostream &os) {
    string chart_node = this->tree_ptr + "[" + this->tree_ptr + BREAK;
    chart_node += "size: " + to_string(this->size) + BREAK;
    chart_node += "]";
    os << chart_node << endl;

    for (int i = 0; i < this->size; i++) {
        auto tree_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
        tree_node->chart(os);
        delete tree_node;
        string link = this->tree_ptr + "-->|";

        if (i == 0)
            link += "x <= " + to_string(this->keys[i]);
        else if (i == this->size - 1) {
            link += to_string(this->keys[i - 1]) + " < x";
        } else {
            link += to_string(this->keys[i - 1]) + " < x <= " + to_string(this->keys[i]);
        }
        link += "|" + this->tree_pointers[i];
        os << link << endl;
    }
}

ostream& InternalNode::write(ostream &os) const {
    TreeNode::write(os);
    for (int i = 0; i < this->size - 1; i++) {
        if (&os == &cout)
            os << "\nP" << i + 1 << ": ";
        os << this->tree_pointers[i] << " ";
        if (&os == &cout)
            os << "\nK" << i + 1 << ": ";
        os << this->keys[i] << " ";
    }
    if (&os == &cout)
        os << "\nP" << this->size << ": ";
    os << this->tree_pointers[this->size - 1];
    return os;
}

istream& InternalNode::read(istream& is) {
    TreeNode::read(is);
    this->keys.assign(this->size - 1, DELETE_MARKER);
    this->tree_pointers.assign(this->size, NULL_PTR);
    for (int i = 0; i < this->size - 1; i++) {
        if (&is == &cin)
            cout << "P" << i + 1 << ": ";
        is >> this->tree_pointers[i];
        if (&is == &cin)
            cout << "K" << i + 1 << ": ";
        is >> this->keys[i];
    }
    if (&is == &cin)
        cout << "P" << this->size;
    is >> this->tree_pointers[this->size - 1];
    return is;
}
