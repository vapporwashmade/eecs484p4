#include "Join.hpp"

#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
    uint partition_b = MEM_SIZE_IN_PAGE - 1;
    vector<Bucket> partitions(partition_b, Bucket(disk));

    // buffers
    // put buffers in mem pages [0, MEM_SIZE_IN_PAGE - 1), and current page in MEM_SIZE_IN_PAGE - 1
    // this piece of code helps readability by giving mem page i an alias of buffers[i]
    vector<Page*> buffers(partition_b);
    for (uint i = 0; i < partition_b; ++i) {
        buffers[i] = mem->mem_page(i);
    }

    // loop through left relation pages
    for (uint i = left_rel.first; i < left_rel.second; ++i) {
        mem->loadFromDisk(disk, i, partition_b);
        Page *page = mem->mem_page(partition_b);

        for (uint j = 0; j < page->size(); ++j) {
            Record r = page->get_record(j);
            uint bucket = r.partition_hash() % partition_b;
            Page *buffer = buffers[bucket];
            if (buffer->full()) {
                uint disk_page = mem->flushToDisk(disk, bucket);
                partitions[bucket].add_left_rel_page(disk_page);
            }
            buffer->loadRecord(r);
        }
    }

    // flush buffers to disk to reuse
    for (uint i = 0; i < partition_b; ++i) {
        if (!buffers[i]->empty()) {
            uint disk_page = mem->flushToDisk(disk, i);
            partitions[i].add_left_rel_page(disk_page);
        }
    }

    // loop through right relation pages
    for (uint i = right_rel.first; i < right_rel.second; ++i) {
        mem->loadFromDisk(disk, i, partition_b);
        Page *page = mem->mem_page(partition_b);

        for (uint j = 0; j < page->size(); ++j) {
            Record r = page->get_record(j);
            uint bucket = r.partition_hash() % partition_b;
            Page *buffer = buffers[bucket];
            if (buffer->full()) {
                uint disk_page = mem->flushToDisk(disk, bucket);
                partitions[bucket].add_right_rel_page(disk_page);
            }
            buffer->loadRecord(r);
        }
    }

    // flush buffers to disk
    for (uint i = 0; i < partition_b; ++i) {
        if (!buffers[i]->empty()) {
            uint disk_page = mem->flushToDisk(disk, i);
            partitions[i].add_right_rel_page(disk_page);
        }
    }

    mem->reset();

	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {

	vector<uint> disk_pages;
    uint probe_b = MEM_SIZE_IN_PAGE - 2;

    Page *output = mem->mem_page(probe_b + 1);
    vector<Page*> buffers(probe_b);
    for (uint i = 0; i < probe_b; ++i) {
        buffers[i] = mem->mem_page(i);
    }

    for (size_t i = 0; i < partitions.size(); ++i) {
        // find bigger and smaller relations
        vector<uint> big = partitions[i].get_left_rel();
        vector<uint> small = partitions[i].get_right_rel();

        if (partitions[i].num_right_rel_record > partitions[i].num_left_rel_record) {
            big = partitions[i].get_right_rel();
            small = partitions[i].get_left_rel();
        }

        for (size_t j = 0; j < small.size(); ++j) {
            mem->loadFromDisk(disk, small[j], probe_b);
            Page *input = mem->mem_page(probe_b);

            for (uint k = 0; k < input->size(); ++k) {
                Record r = input->get_record(k);
                uint bucket = r.probe_hash() % probe_b;
                Page *buffer = buffers[bucket];
                buffer->loadRecord(r);
            }

            // page->reset();
        }

        for (size_t j = 0; j < big.size(); ++j) {
            mem->loadFromDisk(disk, big[j], probe_b);
            Page *input = mem->mem_page(probe_b);

            for (uint k = 0; k < input->size(); ++k) {
                Record r = input->get_record(k);
                uint bucket = r.probe_hash() % probe_b;
                Page *buffer = buffers[bucket];

                // search buffer for equals
                for (uint l = 0; l < buffer->size(); ++l) {
                    Record r2 = buffer->get_record(l);
                    if (r == r2) {
                        if (output->full()) {
                            uint disk_page = mem->flushToDisk(disk, probe_b + 1);
                            disk_pages.push_back(disk_page);
                        }
                        output->loadPair(r, r2);
                    }
                }
            }

            // page->reset();
        }

        for (size_t j = 0; j < probe_b + 1; ++j) {
            mem->mem_page(j)->reset();
        }

    }

    if (!output->empty()) {
        uint disk_page = mem->flushToDisk(disk, probe_b + 1);
        disk_pages.push_back(disk_page);
    }

    mem->reset();

	return disk_pages;
}
