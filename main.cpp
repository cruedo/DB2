#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <cmath>

using namespace std;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define MV_VOID(x, y) (((uint8_t*)(x))+(y))


const uint32_t PAGE_SIZE = 60;
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
const uint32_t MAX_INTERNAL_ROWS = BODY_SIZE / INTERNAL_CELL_SIZE - 2;

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


    int findEmptyPage() {
        fd.seekg(0, ios_base::end);
        int fileSize = fd.tellg();
        int res = ceil((double)fileSize / PAGE_SIZE);
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
            int totalPages = ceil((double)fileSize / PAGE_SIZE);
            
            // If the page with the given number exists within the file then just read it from the file.
            if(index < totalPages) {
                fd.seekg(index * PAGE_SIZE);
                fd.read((char*)(pages[index]->page), PAGE_SIZE);
            }

            if(index == 0) {
                pages[index]->initializeLeafNode();
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


    // Debug
    void printInternalNode(int index) {
        cout << index << " --> ";
        int len = pages[index]->size();
        for(int i = 1; i < len; i += 2) {
            int64_t val;
            memcpy(&val, MV_VOID(pages[index]->page, i * sizeof(int64_t) + BODY_OFFSET), sizeof(int64_t));
            cout << val << ",";
        }
        cout << "\n";
    }
    void printLeafNode(int index) {
        cout << index << " <--> ";
        int len = pages[index]->size();
        for(int i = 0; i < len; ++i) {
            int64_t val = pages[index]->getLeafKey(i);
            cout << val << ",";
        }
        cout << "\n";
    }
    void printAll() {
        int index = findPage(root, -2e8);
        while(index >= 0) {
            printLeafNode(index);
            index = pages[index]->getNext();
        }
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
        printf("debug: \n");
        printInternalNode(pageNumber);

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
        printLeafNode(pageNumber);
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
            int rightHalfPageNumber = splitLeafNode(pageNumber, mid);
            pages[rightHalfPageNumber]->setNext(pg->getNext());
            pg->setNext(rightHalfPageNumber);
            insertIntoInternal(pg->parent(), midKey, pageNumber, rightHalfPageNumber);
        }
    }

    void insert(Row& row) {
        int pageNumber = findPage(root, row.id);
        insertIntoLeaf(pageNumber, row);
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
        table->fd.seekg(i * PAGE_SIZE);
        printf("FILE SZ: %d i: %d\n", table->fd.tellg(), i);
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
    table->printAll();
    return 0;
}

int executeInsert(Table* table, Row& row) {
    table->insert(row);
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

    printf("MAX_LEAF_ROWS: %d\n", MAX_LEAF_ROWS);
    printf("maxinternalrows: %d\n", MAX_INTERNAL_ROWS);

    string rawInputString;
    while(true) {
        cout << "db2 > ";
        getline(cin, rawInputString);
        if(rawInputString.empty()) 
            continue;

        vector<string> inputCommand = split(rawInputString, ' ');
        
        if(rawInputString[0] == '.') {
            switch(doMetaCommand(table, inputCommand)) {
                case 0:
                    continue;
                case 1:
                    cout << "Error: Unrecognized command \" " << inputCommand[0] << " \"" << "\n";
                    continue;
            }
        }


        Row row;
        switch(prepareStatement(inputCommand, row)) {
            case 0:
                break;
            case 1:
                cout << "Error in Prepare !!";
                exit(1);
        }

        switch(executeStatement(table, inputCommand, row)) {
            case 0:
                continue;
            case 1:
                cout << "ERROR in execute !!";
                exit(1);
        }

    }

    return 0;
}