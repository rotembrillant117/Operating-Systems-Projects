#include "VirtualMemory.h"
#include "PhysicalMemory.h"

/**
 * Fills in tables as needed, preparing for write and read
 * @param virtualAddress The vm address
 * @param offset The offset
 * @param addr The frame index
 */
void tableFiller(uint64_t virtualAddress, uint64_t &offset, word_t &addr);

/**
 * Clears the table in the given frame index
 * @param frameIndex The frame index
 */
void clearTable(uint64_t frameIndex) {
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
        PMwrite(frameIndex * PAGE_SIZE + i, 0);
    }
}

/**
 * Initialize the virtual memory
 */
void VMinitialize()
{
    clearTable(0);
}

/**
 * Seperates the address into partitions
 * @param vAddress The address to separate
 * @param partitions The pointer to the array
 */
void createPartitions(uint64_t vAddress, uint64_t * partitions)
{
    for (int i = TABLES_DEPTH  - 1; i >= 0; i--)
    {
        partitions[i] = vAddress & (PAGE_SIZE - 1);
        vAddress = vAddress >> OFFSET_WIDTH;
    }
}

/**
 * @param frameIndex The current frame we are operating on
 * @param maxVal The max frame we've found so far traversing the tree
 * @param routeWeight The weight from the root to the current node
 * @param maxFrameLeaf The frame of the highest weighted route to a leaf node found thus far
 * @param maxWeight The highest weighted route to a leaf node found thus far
 * @param forbiddenFruit The frame we are forbidden to change/remove
 * @param depth The depth of the current node
 * @param parentAdd The address of the parent of the current node
 * @param parentCandidate2 The parent node for case 2
 * @param parentCandidate3 The parent node for case 3
 * @param curPage The current page path
 * @param maxPage The page number of the max weighted route to a leaf
 */
void DFS(uint64_t frameIndex, uint64_t & maxVal, uint64_t routeWeight, uint64_t & maxFrameLeaf,
         uint64_t & maxWeight, uint64_t forbiddenFruit, uint64_t depth, uint64_t parentAdd,
         uint64_t & parentCandidate2, uint64_t & parentCandidate3, uint64_t curPage, uint64_t & maxPage)
{
    // counter for checking if the cur frame is all 0's
    int counter = 0;
    for (int i = 0; i < PAGE_SIZE; i++)
    {
        // The value of the current frame were in at row i
        word_t cur;
        PMread(frameIndex * PAGE_SIZE + i, &cur);
        if (cur != 0)
        {
            if (cur > (word_t) maxVal)
            {
                maxVal = cur;
            }
            parentAdd = frameIndex * PAGE_SIZE + i;
            int add1 = cur % 2 == 0 ? WEIGHT_EVEN : WEIGHT_ODD;
            routeWeight = routeWeight + add1;

            // In case we reach the 1 before last depth, we check the max weight to update if needed (for case 3) and return
            if (depth == TABLES_DEPTH - 1)
            {
                uint64_t tmp = curPage;
                curPage = (curPage << OFFSET_WIDTH) + i;
                int add2 = curPage % 2 == 0 ? WEIGHT_EVEN : WEIGHT_ODD;
                routeWeight = routeWeight + add2;
                if (routeWeight > maxWeight)
                {
                    maxFrameLeaf = cur;
                    maxPage = curPage;
                    maxWeight = routeWeight;
                    parentCandidate3 = parentAdd;
                }
                routeWeight = routeWeight - add2 - add1;
                curPage = tmp;
                continue;
            }
            DFS(cur, maxVal, routeWeight, maxFrameLeaf, maxWeight, forbiddenFruit, depth + 1,
                parentAdd, parentCandidate2,parentCandidate3, (curPage << OFFSET_WIDTH) + i, maxPage);
            routeWeight = routeWeight - add1;
        }
        else
        {
            counter++;
        }
        if (counter == PAGE_SIZE && frameIndex != forbiddenFruit)
        {
            parentCandidate2 = parentAdd;
        }
    }
}

/**
 * reads a word from the given virtual address
 * and puts its content in *value.
 *
 * returns 1 on success.
 * returns 0 on failure (if the address cannot be mapped to a physical
 * address for any reason)
 */
int VMread(uint64_t virtualAddress, word_t* value) {
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE)
    {
        return 0;
    }
    uint64_t offset;
    word_t addr;
    tableFiller(virtualAddress, offset, addr);
    PMread(addr * PAGE_SIZE + offset, value);
    return 1;
}

void tableFiller(uint64_t virtualAddress, uint64_t &offset, word_t &addr) {
    offset= ((1ull << OFFSET_WIDTH) - 1ull) & virtualAddress;
    addr= 0;
    uint64_t partitions[TABLES_DEPTH];
    createPartitions(virtualAddress >> OFFSET_WIDTH, partitions);
    word_t oldAddr = 0;
    for (int i = 0; i < TABLES_DEPTH; i++)
    {
        oldAddr = addr;
        PMread(oldAddr * PAGE_SIZE + partitions[i], &addr);
        if (addr == 0)
        {
            uint64_t maxVal = 0;
            uint64_t maxFrameLeaf = 0;
            uint64_t parentCandidate2 = 0;
            uint64_t parentCandidate3 = 0;
            uint64_t maxPage = 0;
            uint64_t maxWeight = 0;
            DFS(0, maxVal, WEIGHT_EVEN, maxFrameLeaf, maxWeight, oldAddr, 0, 0,
                parentCandidate2, parentCandidate3, 0, maxPage);
            if (maxVal < NUM_FRAMES - 1)

            {
                clearTable(maxVal + 1);
                PMwrite(oldAddr * PAGE_SIZE + partitions[i], maxVal + 1);
                addr = maxVal + 1;
            }
            else if (parentCandidate2 != 0)
            {
                word_t curFrame;
                PMread(parentCandidate2, &curFrame);
                PMwrite(parentCandidate2, 0);
                PMwrite(oldAddr * PAGE_SIZE + partitions[i], curFrame);
                addr = curFrame;
            }
            else if (parentCandidate3 != 0)
            {
                word_t curFrame;
                PMread(parentCandidate3, &curFrame);
                PMwrite(parentCandidate3, 0);
                PMevict(curFrame, maxPage);
                clearTable(curFrame);
                PMwrite(oldAddr * PAGE_SIZE + partitions[i], curFrame);
                addr = curFrame;
            }
        }
    }
    PMrestore(addr, virtualAddress >> OFFSET_WIDTH);
}

/**
 * writes a word to the given virtual address
 *
 * returns 1 on success.
 * returns 0 on failure (if the address cannot be mapped to a physical
 * address for any reason)
 */
int VMwrite(uint64_t virtualAddress, word_t value)
{
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE)
    {
        return 0;
    }
    uint64_t offset = 0;
    word_t addr = 0;
    tableFiller(virtualAddress, offset, addr);
    PMwrite(addr * PAGE_SIZE + offset, value);
    return 1;
}
