// tree.c - Tree object implementation

#include "pes.h"
#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

// Forward declarations from object.c (no object.h exists)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out);

// ─── PROVIDED: tree_parse ────────────────────────────────────────────────────

int tree_parse(const void *data, size_t len, Tree *tree_out) {
    const char *ptr = (const char *)data;
    const char *end = ptr + len;
    tree_out->count = 0;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        while (ptr < end && (*ptr == '\n' || *ptr == '\r')) ptr++;
        if (ptr >= end) break;

        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // Parse mode (octal)
        char mode_str[8];
        int i = 0;
        while (ptr < end && *ptr != ' ' && i < 7)
            mode_str[i++] = *ptr++;
        mode_str[i] = '\0';
        entry->mode = (uint32_t)strtoul(mode_str, NULL, 8);

        if (ptr >= end || *ptr != ' ') return -1;
        ptr++;

        // Parse type string (blob/tree) — skip it
        char type_str[16];
        i = 0;
        while (ptr < end && *ptr != ' ' && i < 15)
            type_str[i++] = *ptr++;
        type_str[i] = '\0';

        if (strcmp(type_str, "blob") != 0 && strcmp(type_str, "tree") != 0)
            return -1;

        if (ptr >= end || *ptr != ' ') return -1;
        ptr++;

        // Parse 64-char hex hash
        if (ptr + HASH_HEX_SIZE > end) return -1;
        if (hex_to_hash(ptr, &entry->hash) != 0) return -1;
        ptr += HASH_HEX_SIZE;

        if (ptr >= end || *ptr != ' ') return -1;
        ptr++;

        // Parse name
        i = 0;
        while (ptr < end && *ptr != '\n' && *ptr != '\r' && i < 255)
            entry->name[i++] = *ptr++;
        entry->name[i] = '\0';

        while (ptr < end && (*ptr == '\n' || *ptr == '\r')) ptr++;
        tree_out->count++;
    }
    return 0;
}

// ─── PROVIDED: tree_serialize ────────────────────────────────────────────────

static int tree_entry_cmp(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    // Make a sorted local copy — spec requires entries sorted by name
    Tree sorted = *tree;
    qsort(sorted.entries, (size_t)sorted.count, sizeof(TreeEntry), tree_entry_cmp);

    // Each line: "NNNNNN type 64hexhash name\n"
    // Max line = 6 + 1 + 4 + 1 + 64 + 1 + 255 + 1 = 333 bytes
    size_t buf_size = (size_t)sorted.count * 333 + 1;
    char *buffer = malloc(buf_size);
    if (!buffer) return -1;

    char hash_hex[HASH_HEX_SIZE + 1];
    char *ptr = buffer;

    for (int i = 0; i < sorted.count; i++) {
        const TreeEntry *e = &sorted.entries[i];
        const char *type_str = (e->mode == 040000) ? "tree" : "blob";
        hash_to_hex(&e->hash, hash_hex);

        int written = snprintf(ptr, buf_size - (size_t)(ptr - buffer),
                               "%o %s %s %s\n",
                               e->mode, type_str, hash_hex, e->name);
        if (written < 0) { free(buffer); return -1; }
        ptr += written;
    }

    *data_out = buffer;
    *len_out  = (size_t)(ptr - buffer);
    return 0;
}

// ─── tree_from_index ─────────────────────────────────────────────────────────

static int build_tree_recursive(const Index *index,
                                 const char  *prefix,
                                 ObjectID    *out_id)
{
    Tree local;
    local.count = 0;

    char seen_dirs[MAX_TREE_ENTRIES][256];
    int  seen_count = 0;

    size_t prefix_len = strlen(prefix);

    for (int i = 0; i < index->count; i++) {
        const IndexEntry *ie = &index->entries[i];

        const char *rel;
        if (prefix_len == 0) {
            rel = ie->path;
        } else {
            if (strncmp(ie->path, prefix, prefix_len) != 0) continue;
            if (ie->path[prefix_len] != '/') continue;
            rel = ie->path + prefix_len + 1;
        }

        if (rel[0] == '\0') continue;

        const char *slash = strchr(rel, '/');

        if (slash == NULL) {
            // Direct file in this directory
            if (local.count >= MAX_TREE_ENTRIES) return -1;
            TreeEntry *te = &local.entries[local.count++];
            te->mode = ie->mode;
            te->hash = ie->hash;
            strncpy(te->name, rel, 255);
            te->name[255] = '\0';

        } else {
            // Belongs to a subdirectory — get immediate child dir name
            size_t dir_len = (size_t)(slash - rel);
            if (dir_len >= 256) return -1;

            char dir_name[256];
            memcpy(dir_name, rel, dir_len);
            dir_name[dir_len] = '\0';

            // Skip if already processed
            int already = 0;
            for (int j = 0; j < seen_count; j++) {
                if (strcmp(seen_dirs[j], dir_name) == 0) { already = 1; break; }
            }
            if (already) continue;

            // Build child prefix and recurse
            char child_prefix[768];
            if (prefix_len == 0)
                snprintf(child_prefix, sizeof(child_prefix), "%s", dir_name);
            else
                snprintf(child_prefix, sizeof(child_prefix), "%s/%s", prefix, dir_name);

            ObjectID subtree_id;
            if (build_tree_recursive(index, child_prefix, &subtree_id) != 0)
                return -1;

            if (local.count >= MAX_TREE_ENTRIES) return -1;
            TreeEntry *te = &local.entries[local.count++];
            te->mode = 040000;
            te->hash = subtree_id;
            strncpy(te->name, dir_name, 255);
            te->name[255] = '\0';

            strncpy(seen_dirs[seen_count], dir_name, 255);
            seen_dirs[seen_count][255] = '\0';
            seen_count++;
        }
    }

    // tree_serialize handles sorting internally
    void  *tree_data;
    size_t tree_len;
    if (tree_serialize(&local, &tree_data, &tree_len) != 0) return -1;

    int rc = object_write(OBJ_TREE, tree_data, tree_len, out_id);
    free(tree_data);
    return rc;
}

int tree_from_index(ObjectID *root_id) {
    Index index;
    if (index_load(&index) != 0) {
        fprintf(stderr, "tree_from_index: failed to load index\n");
        return -1;
    }

    if (index.count == 0) {
        Tree empty;
        empty.count = 0;
        void  *data;
        size_t len;
        if (tree_serialize(&empty, &data, &len) != 0) return -1;
        int rc = object_write(OBJ_TREE, data, len, root_id);
        free(data);
        return rc;
    }

    return build_tree_recursive(&index, "", root_id);
}