#include "RecordPtr.hpp"
#include "LeafNode.hpp"

LeafNode::LeafNode(const TreePtr &tree_ptr) : TreeNode(LEAF, tree_ptr) {
    this->data_pointers.clear();
    this->next_leaf_ptr = NULL_PTR;
    if(!is_null(tree_ptr))
        this->load();
}

//returns max key within this leaf
Key LeafNode::max() {
    auto it = this->data_pointers.rbegin();
    return it->first;
}

//inserts <key, record_ptr> to leaf. If overflow occurs, leaf is split
//split node is returned
/*
Insert key in the correct place, split if needed

*/

//TODO: LeafNode::insert_key to be implemented
TreePtr LeafNode::insert_key(const Key &key, const RecordPtr &record_ptr) {
    TreePtr new_leaf = NULL_PTR; //if leaf is split, new_leaf = ptr to new split node ptr
    // cout << "LeafNode::insert_key not implemented" << endl;

    // add to the data_pointers map, ordered automatically
    this->data_pointers.insert(pair<Key, RecordPtr>(key, record_ptr));
    this->size++;

    // we need to split
    if (this->size > FANOUT) {
        LeafNode* leaf = (LeafNode *) TreeNode::tree_node_factory(LEAF);    // generate a new leaf node (split_node)
        
        // redistribute elements among original and new leaf node
        for (auto it = this->data_pointers.begin(); it != this->data_pointers.end(); it++) {
            if (std::distance(this->data_pointers.begin(), it) < MIN_OCCUPANCY) {
                // i++;
                continue;
            }
            else{
                leaf->data_pointers.insert(make_pair(it->first, it->second));
                this->data_pointers.erase(it--);
            }
        }
        
        // size bookkeeping
        leaf->size = leaf->data_pointers.size();
        this->size = this->data_pointers.size();
        this->next_leaf_ptr = leaf->tree_ptr;       // linking w/ sibling
        leaf->dump();
        new_leaf = leaf->tree_ptr;
        delete leaf;
    }
    // update current leaf
    this->dump();

    // if there is no split, new_leaf is NULL_PTR (-), else it is the pointer to the newly created leaf
    // thus, with no split, the recusrion ends
    return new_leaf;
}

//key is deleted from leaf if exists
//TODO: LeafNode::delete_key to be implemented
void LeafNode::delete_key(const Key &key) {
    // cout << "LeafNode::delete_key not implemented" << endl;
    for (auto it = this->data_pointers.begin(); it != this->data_pointers.end(); it++) {
        if (it->first == key) {
            this->data_pointers.erase(it);
            this->size--;
            break;
        }
    }
    this->dump();
}

//runs range query on leaf
void LeafNode::range(ostream &os, const Key &min_key, const Key &max_key) const {
    BLOCK_ACCESSES++;
    for(const auto& data_pointer : this->data_pointers){
        if(data_pointer.first >= min_key && data_pointer.first <= max_key)
            data_pointer.second.write_data(os);
        if(data_pointer.first > max_key)
            return;
    }
    if(!is_null(this->next_leaf_ptr)){
        auto next_leaf_node = new LeafNode(this->next_leaf_ptr);
        next_leaf_node->range(os, min_key, max_key);
        delete next_leaf_node;
    }
}

//exports node - used for grading
void LeafNode::export_node(ostream &os) {
    TreeNode::export_node(os);
    for(const auto& data_pointer : this->data_pointers){
        os << data_pointer.first << " ";
    }
    os << endl;
}

//writes leaf as a mermaid chart
void LeafNode::chart(ostream &os) {
    string chart_node = this->tree_ptr + "[" + this->tree_ptr + BREAK;
    chart_node += "size: " + to_string(this->size) + BREAK;
    for(const auto& data_pointer: this->data_pointers) {
        chart_node += to_string(data_pointer.first) + " ";
    }
    chart_node += "]";
    os << chart_node << endl;
}

ostream& LeafNode::write(ostream &os) const {
    TreeNode::write(os);
    for(const auto & data_pointer : this->data_pointers){
        if(&os == &cout)
            os << "\n" << data_pointer.first << ": ";
        else
            os << "\n" << data_pointer.first << " ";
        os << data_pointer.second;
    }
    os << endl;
    os << this->next_leaf_ptr << endl;
    return os;
}

istream& LeafNode::read(istream& is){
    TreeNode::read(is);
    this->data_pointers.clear();
    for(int i = 0; i < this->size; i++){
        Key key = DELETE_MARKER;
        RecordPtr record_ptr;
        if(&is == &cin)
            cout << "K: ";
        is >> key;
        if(&is == &cin)
            cout << "P: ";
        is >> record_ptr;
        this->data_pointers.insert(pair<Key,RecordPtr>(key, record_ptr));
    }
    is >> this->next_leaf_ptr;
    return is;
}