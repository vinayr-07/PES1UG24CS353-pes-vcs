#include "pes.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <inttypes.h>

// ——— PROVIDED FUNCTIONS —————————————————————————————————————————————

IndexEntry* index_find(Index *index, const char *path) {
    for (size_t i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            return &index->entries[i];
        }
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    IndexEntry *entry = index_find(index, path);
    if (!entry) return -1;
    
    for (size_t i = 0; i < index->count - 1; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            memmove(&index->entries[i], &index->entries[i + 1],
                    (index->count - i - 1) * sizeof(IndexEntry));
            break;
        }
    }
    index->count--;
    return 0;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    if (index->count == 0) {
        printf("  (nothing to show)\n");
    } else {
        for (size_t i = 0; i < index->count; i++) {
            printf("  staged:     %s\n", index->entries[i].path);
        }
    }
    printf("\n");
    return 0;
}

// ——— TODO: IMPLEMENT THESE ————————————————————————————————————————

int index_load(Index *index) {
    index->count = 0;
    
    FILE *fp = fopen(".pes/index", "r");
    if (!fp) {
        return 0;  // File doesn't exist - OK, return empty index
    }
    
    char line[1024];
    while (fgets(line, sizeof(line), fp) && index->count < MAX_INDEX_ENTRIES) {
        IndexEntry *entry = &index->entries[index->count];
        char hash_hex[HASH_HEX_SIZE + 1];
        
        // Parse: <mode> <hash> <mtime> <size> <path>
        if (sscanf(line, "%o %64s %" SCNu64 " %" SCNu32 " %511s",
                   &entry->mode, hash_hex, &entry->mtime_sec,
                   &entry->size, entry->path) == 5) {
            hex_to_hash(hash_hex, &entry->hash);
            index->count++;
        }
    }
    
    fclose(fp);
    return 0;
}

int index_save(const Index *index) {
    if (index->count == 0) {
        unlink(".pes/index");
        return 0;
    }
    
    char temp_path[] = ".pes/index.tmp.XXXXXX";
    int fd = mkstemp(temp_path);
    if (fd < 0) return -1;
    
    FILE *fp = fdopen(fd, "w");
    if (!fp) {
        close(fd);
        unlink(temp_path);
        return -1;
    }
    
    // Create sorted copy
    IndexEntry *sorted_entries = malloc(index->count * sizeof(IndexEntry));
    if (!sorted_entries) {
        fclose(fp);
        unlink(temp_path);
        return -1;
    }
    
    memcpy(sorted_entries, index->entries, index->count * sizeof(IndexEntry));
    
    // Simple bubble sort by path
    for (size_t i = 0; i < index->count - 1; i++) {
        for (size_t j = 0; j < index->count - i - 1; j++) {
            if (strcmp(sorted_entries[j].path, sorted_entries[j+1].path) > 0) {
                IndexEntry temp = sorted_entries[j];
                sorted_entries[j] = sorted_entries[j+1];
                sorted_entries[j+1] = temp;
            }
        }
    }
    
    // Write entries
    for (size_t i = 0; i < index->count; i++) {
        IndexEntry *entry = &sorted_entries[i];
        char hash_hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&entry->hash, hash_hex);
        
        fprintf(fp, "%o %s %" PRIu64 " %" PRIu32 " %s\n",
                entry->mode, hash_hex, entry->mtime_sec,
                entry->size, entry->path);
    }
    
    free(sorted_entries);
    
    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);
    
    if (rename(temp_path, ".pes/index") != 0) {
        unlink(temp_path);
        return -1;
    }
    
    return 0;
}

int index_add(Index *index, const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "Error: Cannot open file '%s'\n", path);
        return -1;
    }
    
    fseek(fp, 0, SEEK_END);
    size_t size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    
    void *content = malloc(size);
    if (!content) {
        fclose(fp);
        return -1;
    }
    
    size_t bytes_read = fread(content, 1, size, fp);
    fclose(fp);
    
    if (bytes_read != size) {
        free(content);
        return -1;
    }
    
    struct stat st;
    if (stat(path, &st) != 0) {
        free(content);
        return -1;
    }
    
    uint32_t mode = 100644;
    if (st.st_mode & S_IXUSR) {
        mode = 100755;
    }
    
    ObjectID blob_id;
    if (object_write(OBJ_BLOB, content, size, &blob_id) != 0) {
        free(content);
        return -1;
    }
    free(content);
    
    uint64_t mtime = st.st_mtime;
    
    IndexEntry *existing = index_find(index, path);
    if (existing) {
        existing->mode = mode;
        existing->hash = blob_id;
        existing->mtime_sec = mtime;
        existing->size = size;
    } else {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "Error: Index full\n");
            return -1;
        }
        
        IndexEntry *entry = &index->entries[index->count++];
        entry->mode = mode;
        entry->hash = blob_id;
        entry->mtime_sec = mtime;
        entry->size = size;
        strncpy(entry->path, path, sizeof(entry->path) - 1);
        entry->path[sizeof(entry->path) - 1] = '\0';
    }
    
    // AUTO-SAVE THE INDEX
    if (index_save(index) != 0) {
        fprintf(stderr, "Error: Failed to save index\n");
        return -1;
    }
    
    return 0;
}