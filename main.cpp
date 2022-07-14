#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <cmath>
#include <queue>
#include <utility>
#include <random>
#include <chrono>

using namespace std;

mt19937 rng(chrono::steady_clock::now().time_since_epoch().count());


#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define MV_VOID(x, y) (((uint8_t*)(x))+(y))


const uint32_t PAGE_SIZE = 70;
const uint32_t MAX_PAGES = 100;
const uint32_t MAX_TABLES = 100;


const uint32_t IS_LEAF_OFFSET = 0;
const uint32_t IS_LEAF_SIZE = sizeof(uint8_t);
const int32_t PARENT_NUM_OFFSET = IS_LEAF_SIZE + IS_LEAF_OFFSET;
const int32_t PARENT_NUM_SIZE = sizeof(int32_t);
const int32_t NEXT_NODE_OFFSET = PARENT_NUM_OFFSET + PARENT_NUM_SIZE;
const int32_t NEXT_NODE_SIZE = sizeof(int32_t);
const uint32_t NUM_CELL_OFFSET = NEXT_NODE_OFFSET + NEXT_NODE_SIZE;
const uint32_t NUM_CELL_SIZE = sizeof(uint32_t);
const uint32_t HEADER_SIZE = NUM_CELL_SIZE + NUM_CELL_OFFSET;
const uint32_t BODY_OFFSET = HEADER_SIZE;
const uint32_t BODY_SIZE = PAGE_SIZE - HEADER_SIZE;

const uint32_t TABLE_NUM = 0;
const uint32_t INTERNAL_CELL_SIZE = sizeof(uint64_t);


void print(void* ptr, int sz) {
    for(int i=0;i<sz;++i) {
        printf("%02x ", *(char*)MV_VOID(ptr, i));
    }
    printf("\n");
}


class Row {
public:
    int64_t id;
    // char name[1];
    // char email[1];
};

const uint32_t ROW_SIZE = sizeof(Row);
const uint32_t MAX_LEAF_ROWS = BODY_SIZE / ROW_SIZE - 1;
const uint32_t MIN_LEAF_ROWS = ceil((double)MAX_LEAF_ROWS / 2);
const uint32_t MAX_INTERNAL_ROWS = BODY_SIZE / INTERNAL_CELL_SIZE - 2 - ((BODY_SIZE / INTERNAL_CELL_SIZE) % 2 == 0);
const uint32_t MAX_INTERNAL_KEYS = MAX_INTERNAL_ROWS / 2;
const uint32_t MIN_INTERNAL_KEYS = ceil((double)MAX_INTERNAL_KEYS / 2);
const uint32_t MIN_INTERNAL_ROWS = 2 * (MIN_INTERNAL_KEYS) + 1;

void printConstants() {
    cout << "\n";
    cout << "IS_LEAF_OFFSET = " << IS_LEAF_OFFSET << "\n";
    cout << "IS_LEAF_SIZE = " << IS_LEAF_SIZE << "\n";
    cout << "PARENT_NUM_OFFSET = " << PARENT_NUM_OFFSET << "\n";
    cout << "PARENT_NUM_SIZE = " << PARENT_NUM_SIZE << "\n";
    cout << "NEXT_NODE_OFFSET = " << NEXT_NODE_OFFSET << "\n";
    cout << "NEXT_NODE_SIZE = " << NEXT_NODE_SIZE << "\n";
    cout << "NUM_CELL_OFFSET = " << NUM_CELL_OFFSET << "\n";
    cout << "NUM_CELL_SIZE = " << NUM_CELL_SIZE << "\n";
    cout << "HEADER_SIZE = " << HEADER_SIZE << "\n";
    cout << "BODY_OFFSET = " << BODY_OFFSET << "\n";
    cout << "BODY_SIZE = " << BODY_SIZE << "\n";
    cout << "INTERNAL_CELL_SIZE = " << INTERNAL_CELL_SIZE << "\n";
    cout << "ROW_SIZE = " << ROW_SIZE << "\n";
    cout << "MAX_LEAF_ROWS = " << MAX_LEAF_ROWS << "\n";
    cout << "MIN_LEAF_ROWS = " << MIN_LEAF_ROWS << "\n";
    cout << "MAX_INTERNAL_ROWS = " << MAX_INTERNAL_ROWS << "\n";
    cout << "MAX_INTERNAL_KEYS = " << MAX_INTERNAL_KEYS << "\n";
    cout << "MIN_INTERNAL_KEYS = " << MIN_INTERNAL_KEYS << "\n";
    cout << "MIN_INTERNAL_ROWS = " << MIN_INTERNAL_ROWS << "\n";
    // cout << " = " <<  << "\n";
    cout << "\n";
}

class PageNode {
public:
    void *page;


    void* getLeafRowByteOffset(int index) {
        return MV_VOID(page, index * ROW_SIZE + BODY_OFFSET);
    }
    void* getInternalRowByteOffset(int index) {
        return MV_VOID(page, index * sizeof(int64_t) + BODY_OFFSET);
    }

    PageNode() {
        page = operator new(PAGE_SIZE);
    }

    void setIsLeaf(uint8_t status) {
        uint8_t* ptr = MV_VOID(page, IS_LEAF_OFFSET);
        memcpy(ptr, &status, IS_LEAF_SIZE);
    }
    uint8_t isLeaf() {
        uint8_t* ptr = MV_VOID(page, IS_LEAF_OFFSET), status;
        memcpy(&status, ptr, IS_LEAF_SIZE);
        return status;
    }
    void setParent(int par) {
        memcpy(MV_VOID(page, PARENT_NUM_OFFSET), &par, PARENT_NUM_SIZE);
    }
    int parent() {
        int status;
        memcpy(&status, MV_VOID(page, PARENT_NUM_OFFSET), PARENT_NUM_SIZE);
        return status;
    }
    void setNumRows(int length) {
        memcpy(MV_VOID(page, NUM_CELL_OFFSET), &length, NUM_CELL_SIZE);
    }
    uint32_t size() {
        int len;
        memcpy(&len, MV_VOID(page, NUM_CELL_OFFSET), NUM_CELL_SIZE);
        return len;
    }
    void setNext(int32_t index) {
        memcpy(MV_VOID(page, NEXT_NODE_OFFSET), &index, NEXT_NODE_SIZE);
    }
    int32_t getNext() {
        int val;
        memcpy(&val, MV_VOID(page, NEXT_NODE_OFFSET), NEXT_NODE_SIZE);
        return val;
    }

    Row getLeafRow(int rowNum) {
        Row val;
        memcpy(&val, getLeafRowByteOffset(rowNum), ROW_SIZE);
        return val;
    }
    int64_t getLeafKey(int rowNum) {
        return getLeafRow(rowNum).id;
    }
    void copyLeafRow(int src, int dest) {
        memcpy(getLeafRowByteOffset(dest), getLeafRowByteOffset(src), ROW_SIZE);
    }
    void setLeafRow(Row& row, int rowNum) {
        memcpy(getLeafRowByteOffset(rowNum), &row, ROW_SIZE);
    }


    int64_t getInternalKey(int index) {
        return *(int64_t*)MV_VOID(page, index * INTERNAL_CELL_SIZE + BODY_OFFSET);
    }
    uint64_t getInternalPointer(int index) {
        return *(uint64_t*)MV_VOID(page, index * INTERNAL_CELL_SIZE + BODY_OFFSET);
    }
    void setInternalKey(int index, int64_t key) {
        *(int64_t*)MV_VOID(page, index * INTERNAL_CELL_SIZE + BODY_OFFSET) = key;
    }
    void setInternalPointer(int index, uint64_t ptr) {
        *(uint64_t*)MV_VOID(page, index * INTERNAL_CELL_SIZE + BODY_OFFSET) = ptr;
    }
    void copyInternalCell(int src, int dest) {
        setInternalKey(dest, getInternalKey(src));
    }
    void insertInternalCell(int index, int64_t value) {
        int len = size();
        for(int i=len-1;i>=index;--i) {
            copyInternalCell(i, i+1);
        }
        setInternalKey(index, value);
        setNumRows(len + 1);
    }
    void eraseInternalCell(int index) {
        int len = size();
        for(int i=index; i<len-1; ++i) {
            copyInternalCell(i+1, i);
        }
        --len;
        setNumRows(len);
    }
    int keySize() {
        return size() / 2;
    }

    void initializeLeafNode() {
        setParent(-1);
        setNext(-1);
        setIsLeaf(1);
    }
};



class Table {
public:
    string name;
    vector<PageNode*> pages;
    int32_t root;
    fstream fd;
    string filename;
    int32_t page_count;


    int findEmptyPage() {
        int res = page_count;
        ++page_count;
        pages[res] = new PageNode();
        return res;
    }
    void loadPage(int index) {

        fd.seekg(0, ios_base::end);
        int fileSize = fd.tellg();
        if(fileSize < 0) {
            cout << "tellp is -1 !\n";
            exit(1);
        }

        if(index >= MAX_PAGES) {
            cout << "Error: page index out of bounds";
            exit(1);
        }
        
        if(pages[index] == nullptr) {
            pages[index] = new PageNode();
            int totalPages = page_count;

            
            // If the page with the given number exists within the file then just read it from the file.
            if(index < totalPages) {
                fd.seekg(index * PAGE_SIZE);
                fd.read((char*)(pages[index]->page), PAGE_SIZE);
            }
            else {
                ++page_count;
                if(index == 0) {
                    pages[index]->initializeLeafNode();
                }
            }

        }
        return ;
    }

    // Search
    int findPage(int curIndex, int64_t x) {
        loadPage(curIndex); // load page from memory

        if(pages[curIndex]->isLeaf())
            return curIndex;

        int ind, len = pages[curIndex]->size();
        for(ind = 1; ind < len; ind+=2) {
            int64_t key = pages[curIndex]->getInternalKey(ind);
            if(key >= x) break;
        }
        ind = pages[curIndex]->getInternalPointer(ind - 1);
        return findPage(ind, x);
    }
    int findRoot(int curIndex) {
        loadPage(curIndex);
        while(pages[curIndex]->parent() != -1) {
            curIndex = pages[curIndex]->parent();
            loadPage(curIndex);
        }
        return curIndex;
    }

    // Debug
    void printInternalNode(int index, queue<pair<int64_t, int64_t>> &Q, int dis) {
        cout << index << "-->";
        auto pg = pages[index];
        int len = pages[index]->size();
        
        for(int i=1;i<len;i+=2) {
            cout << pg->getInternalKey(i) << ",";
        }

        for(int i=0;i<len;i+=2) {
            Q.push({dis+1, pg->getInternalPointer(i)});
        }

        cout << " : ";
    }
    void printLeafNode(int index) {
        cout << index << "<-->";
        int len = pages[index]->size();
        for(int i = 0; i < len; ++i) {
            int64_t val = pages[index]->getLeafKey(i);
            cout << val << ",";
        }
        cout << " : ";
    }
    void printTable() {
        queue<pair<int64_t, int64_t>> Q;
        Q.push({0,root});
        int prev = 0;

        while(Q.size()) {
            auto [dis, cur] = Q.front();
            Q.pop();

            loadPage(cur);

            if(dis != prev) {
                cout << "\n";
            }

            if(pages[cur]->isLeaf()) {
                printLeafNode(cur);
            }
            else {
                printInternalNode(cur, Q, dis);
            }
            prev = dis;
        }
        cout << "\n\n";
    }

    // Insert
    int64_t splitInternalNode(int pageNumber, int index) {
        PageNode* pg = pages[pageNumber];
        int sz = pg->size();
        int right = findEmptyPage();
        pages[right]->setIsLeaf(0);

        int rightSize = 0;
        for(int i=index+1; i<sz; ++i) {
            pages[right]->setInternalKey(rightSize, pg->getInternalKey(i));
            ++rightSize;
        }
        pages[right]->setNumRows(rightSize);
        pg->setNumRows(index);


        for(int i=0; i<rightSize; i+=2) {
            uint64_t ptr = pages[right]->getInternalPointer(i);
            pages[ptr]->setParent(right);
        }

        return right;
    }
    void insertIntoInternal(int pageNumber, int64_t key, int left, int right) {
        PageNode* pg = nullptr;
        if(pageNumber == -1) {
            pageNumber = findEmptyPage();
            pg = pages[pageNumber];
            pg->setParent(-1);
            pg->setIsLeaf(0);
            pg->setInternalPointer(0, left);
            pg->setNumRows(pg->size() + 1);
            root = pageNumber;
        }
        pg = pages[pageNumber];


        pages[left]->setParent(pageNumber);
        pages[right]->setParent(pageNumber);

        int i, sz = pg->size();
        for(i = sz-2; i > 0; i -= 2) {
            if(pg->getInternalKey(i) > key) {
                pg->copyInternalCell(i, i+2);
                pg->copyInternalCell(i+1, i+3);
                continue;
            }
            break;
        }
        i += 2;
        pg->setInternalKey(i, key);
        pg->setInternalPointer(i+1, right);
        pg->setNumRows(pg->size() + 2);

        sz = pg->size();
        if(sz > MAX_INTERNAL_ROWS) {
            int mid = sz / 2;
            if(mid % 2 == 0) --mid;

            right = splitInternalNode(pageNumber, mid);
            int64_t midKey = pg->getInternalKey(mid);
            insertIntoInternal(pg->parent(), midKey, pageNumber, right);
        }
    }

    int64_t splitLeafNode(int pageNumber, int index) {
        int numRightHalfRows = pages[pageNumber]->size() - index;
        pages[pageNumber]->setNumRows(index);

        int rightHalfIndex = findEmptyPage();
        PageNode* pgnd = pages[rightHalfIndex];
        pgnd->setNext(-1);
        pgnd->setIsLeaf(1);
        pgnd->setNumRows(numRightHalfRows);
        memcpy(pgnd->getLeafRowByteOffset(0), pages[pageNumber]->getLeafRowByteOffset(index), numRightHalfRows * ROW_SIZE);
        pages.push_back(pgnd);
        return rightHalfIndex;
    }
    void insertIntoLeaf(int pageNumber, Row& row) {
        int len = pages[pageNumber]->size();
        PageNode* pg = pages[pageNumber];
        int pos;
        for(pos = len-1; pos >= 0; --pos) {
            if(pg->getLeafKey(pos) > row.id)
                pg->copyLeafRow(pos, pos+1);
            else
                break;
        }
        pg->setLeafRow(row, pos+1);
        pg->setNumRows(pg->size() + 1);
        
        int sz = pg->size();

        if(sz > MAX_LEAF_ROWS) {
            int mid = (sz - 1) / 2;
            int64_t midKey = pg->getLeafKey(mid);
            int rightHalfPageNumber = splitLeafNode(pageNumber, mid+1);
            pages[rightHalfPageNumber]->setNext(pg->getNext());
            pg->setNext(rightHalfPageNumber);
            insertIntoInternal(pg->parent(), midKey, pageNumber, rightHalfPageNumber);
        }
    }

    void insert(int x) {
        int pageNumber = findPage(root, x);
        Row row; row.id = x;
        insertIntoLeaf(pageNumber, row);
    }

    // Delete

    void mergeInternalNodes(int leftPageNumber, int rightPageNumber, int mid) {
        PageNode* LPG = pages[leftPageNumber];
        PageNode* RPG = pages[rightPageNumber];

        int Llen = LPG->size(), Rlen = RPG->size();
        LPG->setInternalKey(Llen, mid);
        ++Llen;
        for(int i=0;i<Rlen;++i) {
            LPG->setInternalKey(Llen, RPG->getInternalKey(i));
            ++Llen;
            if(i % 2 == 0) {
                pages[RPG->getInternalPointer(i)]->setParent(leftPageNumber);
            }
        }
        LPG->setNumRows(Llen);

        // WARNING !! Delete the right node here.
    }

    // REVIEW REQUIRED !!
    void deleteInternal(int pageNumber, int key, int index) {
        PageNode* pgnd = pages[pageNumber];
        int len = pgnd->size();

        for(int i=index; i<len-2; ++i) {
            pgnd->copyInternalCell(i+2, i);
        }
        len -= 2;
        pgnd->setNumRows(len);

        if(len == 1 && pgnd->parent() == -1) {
            int loneChildPageNumber = pgnd->getInternalPointer(0);
            // De-allocate page with "pageNumber" here !
            root = loneChildPageNumber;
            pages[root]->setParent(-1);
            return;
        }
        if(pages[pageNumber]->parent() == -1 || len / 2 >= MIN_INTERNAL_KEYS) {
            return;
        }

        int leftSiblingPageNumber = -1, rightSiblingPageNumber = -1, parentPageNumber = pgnd->parent();
        int parentLen = pages[parentPageNumber]->size(), Llen = -1, Rlen = -1;

        int ind;
        for(ind=1;ind<parentLen;ind+=2) {
            if(pages[parentPageNumber]->getInternalKey(ind) >= key)
                break;
        }
        --ind;

        if(ind-2 >= 0) {leftSiblingPageNumber = pages[parentPageNumber]->getInternalPointer(ind-2); Llen = pages[leftSiblingPageNumber]->size();}
        if(ind+2 < parentLen) {rightSiblingPageNumber = pages[parentPageNumber]->getInternalPointer(ind+2);  pages[rightSiblingPageNumber]->size();}

        if(leftSiblingPageNumber != -1 && pages[leftSiblingPageNumber]->keySize() > MIN_INTERNAL_KEYS) {
            pages[pageNumber]->insertInternalCell(0, pages[parentPageNumber]->getInternalKey(ind-1));
            pages[pageNumber]->insertInternalCell(0, pages[leftSiblingPageNumber]->getInternalPointer(Llen-1));
            pages[pages[leftSiblingPageNumber]->getInternalPointer(Llen-1)]->setParent(pageNumber);
            --Llen;
            pages[leftSiblingPageNumber]->setNumRows(Llen);
            pages[parentPageNumber]->setInternalKey(ind-1, pages[leftSiblingPageNumber]->getInternalKey(Llen-1));
            --Llen;
            pages[leftSiblingPageNumber]->setNumRows(Llen);
        }
        else if(rightSiblingPageNumber != -1 && pages[rightSiblingPageNumber]->keySize() > MIN_INTERNAL_KEYS) {
            pages[pageNumber]->insertInternalCell(len, pages[parentPageNumber]->getInternalKey(ind+1));
            ++len;
            pages[pageNumber]->insertInternalCell(len, pages[rightSiblingPageNumber]->getInternalPointer(0));
            pages[pages[rightSiblingPageNumber]->getInternalPointer(0)]->setParent(pageNumber);
            pages[rightSiblingPageNumber]->eraseInternalCell(0);
            pages[parentPageNumber]->setInternalKey(ind+1, pages[rightSiblingPageNumber]->getInternalKey(0)); // Keys have shifted to even positions due to the deletion in the previous line
            pages[rightSiblingPageNumber]->eraseInternalCell(0);
        }
        else if(leftSiblingPageNumber != -1) {
            mergeInternalNodes(leftSiblingPageNumber, pageNumber, pages[parentPageNumber]->getInternalKey(ind-1));
            deleteInternal(parentPageNumber, pages[parentPageNumber]->getInternalKey(ind-1), ind-1);
        }
        else if(rightSiblingPageNumber != -1) {
            mergeInternalNodes(pageNumber, rightSiblingPageNumber, pages[parentPageNumber]->getInternalKey(ind+1));
            deleteInternal(parentPageNumber, pages[parentPageNumber]->getInternalKey(ind+1), ind+1);
        }
    }

    void mergeLeafNodes(int leftPageNumber, int rightPageNumber) {
        int rightLen = pages[rightPageNumber]->size();
        int leftLen = pages[leftPageNumber]->size();
        Row row;

        for(int i=0; i<rightLen; ++i) {
            row = pages[rightPageNumber]->getLeafRow(i);
            pages[leftPageNumber]->setLeafRow(row, leftLen);
            ++leftLen;
        }
        pages[leftPageNumber]->setNumRows(leftLen);

        pages[leftPageNumber]->setNext(pages[rightPageNumber]->getNext());
        // WARNING !! delete the right page here
    }

    void deleteLeaf(int pageNumber, int key) {
        PageNode* pgnd = pages[pageNumber];
        int len = pgnd->size();
        int data_index;
        for(data_index = 0; data_index < len; ++data_index) {
            if(pgnd->getLeafKey(data_index) == key)
                break;
        }

        if(data_index == len) {
            cout << "Error: Key does not exist\n";
            return;
        }

        int numOfRowsToMove = len - data_index - 1;
        for(int ind = data_index+1; ind < len; ++ind) {
            pgnd->copyLeafRow(ind, ind-1);
        }

        len -= 1;
        pgnd->setNumRows(len);


        if(pgnd->parent() == -1 || len >= MIN_LEAF_ROWS) {
            return;
        }

        int leftSiblingPageNumber = -1, rightSiblingPageNumber = -1, parentPageNumber = pgnd->parent();

        // "ind" is the index of the 'pointer to the current page' in the parent's key-pointer array.
        // It is used to find the page numbers of the sibling nodes
        int ind;

        int parentDataSize = pages[parentPageNumber]->size();
        for(ind = 1; ind < parentDataSize; ind += 2) {
            if(pages[parentPageNumber]->getInternalKey(ind) >= key)
                break;
        }
        --ind;
        if(ind-2 >= 0) leftSiblingPageNumber = pages[parentPageNumber]->getInternalPointer(ind - 2);
        if(ind+2 < parentDataSize) rightSiblingPageNumber = pages[parentPageNumber]->getInternalPointer(ind + 2);

        if(leftSiblingPageNumber != -1 && pages[leftSiblingPageNumber]->size() > MIN_LEAF_ROWS) {
            int leftS_len = pages[leftSiblingPageNumber]->size();
            Row row = pages[leftSiblingPageNumber]->getLeafRow(leftS_len - 1);

            // Update the left Sibling
            --leftS_len;
            pages[leftSiblingPageNumber]->setNumRows(leftS_len);

            // Update the current node with the borrowed value
            for(int i=len;i>0;--i) {
                pgnd->copyLeafRow(i-1, i);
            }
            pgnd->setLeafRow(row, 0);
            ++len;
            pgnd->setNumRows(len);

            // Update the parent
            pages[parentPageNumber]->setInternalPointer(ind - 1, pages[leftSiblingPageNumber]->getLeafKey(leftS_len-1));

        }
        else if(rightSiblingPageNumber != -1 && pages[rightSiblingPageNumber]->size() > MIN_LEAF_ROWS) {
            int rightS_len = pages[rightSiblingPageNumber]->size();
            Row row = pages[rightSiblingPageNumber]->getLeafRow(0);

            // Update the parent
            pages[parentPageNumber]->setInternalPointer(ind + 1, pages[rightSiblingPageNumber]->getLeafKey(0));

            // Update the current node with the borrowed value
            pgnd->setLeafRow(row, len);
            ++len;
            pgnd->setNumRows(len);

            // Update the right Sibling
            for(int i=0; i<rightS_len-1; ++i) {
                pages[rightSiblingPageNumber]->copyLeafRow(i+1, i);
            }
            --rightS_len;
            pages[rightSiblingPageNumber]->setNumRows(rightS_len);

        }
        else if(leftSiblingPageNumber != -1) {
            mergeLeafNodes(leftSiblingPageNumber, pageNumber);
            deleteInternal(parentPageNumber, pages[parentPageNumber]->getInternalKey(ind-1), ind-1);
        }
        else if(rightSiblingPageNumber != -1) {
            mergeLeafNodes(pageNumber, rightSiblingPageNumber);
            deleteInternal(parentPageNumber, pages[parentPageNumber]->getInternalKey(ind+1), ind+1);
        }


    }

    void deleteData(int x) {
        int pageNumber = findPage(root, x);
        deleteLeaf(pageNumber, x);
    }
};








Table* table_open(char* filename) {
    Table* table = new Table;
    table->root = 0;
    table->filename = string(filename);
    (table->fd).open(table->filename, ios::out | ios::in );
    table->pages.resize(PAGE_SIZE);
    for(auto &p: table->pages) {
        p = nullptr;
    }
    (table->fd).seekg(0, ios_base::end);
    int fileSize = (table->fd).tellg();
    table->page_count = ceil((double)fileSize / PAGE_SIZE);
    cout << "The total pages are : " << table->page_count << "\n";
    table->root = table->findRoot(0);
    cout << "The root is initialized to : " << table->root << "\n";

    return table;
}

int table_close(Table* table) {
    for(int i = 0; i < PAGE_SIZE ; ++i) {
        auto &p = table->pages[i];
        if(p == nullptr) continue;
        if(p->isLeaf()) {
            table->printLeafNode(i);
            // print(p->page, 40);
        }
        table->fd.seekp(i * PAGE_SIZE);
        table->fd.write((char*)(p->page), PAGE_SIZE);
    }
    table->fd.close();
    return 0;
}

int doMetaCommand(Table* table, vector<string> &inputCommand) {
    if(inputCommand[0] == ".exit") {
        table_close(table);
        exit(0);
    }
    return 1;
}

int prepareStatement(vector<string> &inputCommand, Row& row) {
    string command = inputCommand[0];
    if(command == "insert" && inputCommand.size() >= 4) {
        row.id = atoll(inputCommand[1].c_str());
        // row.name = inputCommand[2];
        // row.email = inputCommand[3];
        return 0;
    }
    else if(command == "select") {
        return 0;
    }
    return 1;
}

int executeSelect(Table* table) {
    return 0;
}

int executeInsert(Table* table, Row& row) {
    // table->insert(row);
    return 0;
}

int executeStatement(Table* table, vector<string> &inputCommand, Row& row) {
    if(inputCommand[0] == "insert") {
        return executeInsert(table, row);
    }
    else if(inputCommand[0] == "select") {
        return executeSelect(table);
    }
    return 1;
}

vector<string> split(string& input, char delimitter) {
    input.push_back(delimitter);
    vector<string> output;
    string tmp;
    for(auto c: input) {
        if(c == delimitter) {
            output.push_back(tmp);
            tmp.clear();
            continue;
        }
        tmp.push_back(c);
    }
    return output;
}

int main(int argc, char* argv[]) {
    if(argc < 2) {
        cout << "Error: Database file not provided !\n";
        exit(1);
    }
    char* filename = argv[1];

    Table* table = table_open(filename);

    printConstants();

    string rawInputString;


    vector<int> v;
    for(int i=0;i<54;++i) {
        v.push_back(i);
    }
    vector<int> tmp = v;

    
    while(tmp.size()) {
        int m = tmp.size();
        int val = rng() % m;
        cout << "INSERT: " << tmp[val] << "\n";
        table->insert(tmp[val]);
        tmp.erase(tmp.begin() + val);
        table->printTable();
    }

    tmp = v;

    cout << "\n\n\n";

    while(tmp.size()) {
        int m = tmp.size();
        int val = rng() % m;
        cout << "DELETE: " << tmp[val] << "\n";
        table->deleteData(tmp[val]);
        tmp.erase(tmp.begin() + val);
        table->printTable();
    }


    // while(true) {
    //     cout << "db2 > ";
    //     getline(cin, rawInputString);
    //     if(rawInputString.empty()) 
    //         continue;

    //     vector<string> inputCommand = split(rawInputString, ' ');
        
    //     if(rawInputString[0] == '.') {
    //         switch(doMetaCommand(table, inputCommand)) {
    //             case 0:
    //                 continue;
    //             case 1:
    //                 cout << "Error: Unrecognized command \" " << inputCommand[0] << " \"" << "\n";
    //                 continue;
    //         }
    //     }


    //     Row row;
    //     switch(prepareStatement(inputCommand, row)) {
    //         case 0:
    //             break;
    //         case 1:
    //             cout << "Error in Prepare !!";
    //             exit(1);
    //     }

    //     switch(executeStatement(table, inputCommand, row)) {
    //         case 0:
    //             continue;
    //         case 1:
    //             cout << "ERROR in execute !!";
    //             exit(1);
    //     }

    // }




    return 0;
}