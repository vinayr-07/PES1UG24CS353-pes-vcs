// PES-VCS — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:    object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ——— PROVIDED ————————————————————————————————————————————————————————

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%02x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ——— TODO: Implement these ———————————————————————————————————————————

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
// where <type> is "blob", "tree", or "commit"
// and <size> is the decimal string of the data length
//
// Steps:
// 1. Build the full object: header ("blob 16\0") + data
// 2. Compute SHA-256 hash of the FULL object (header + data)
// 3. Check if object already exists (deduplication) — if so, just return success
// 4. Create shard directory (.pes/objects/XX/) if it doesn't exist
// 5. Write to a temporary file in the same shard directory
// 6. fsync() the temporary file to ensure data reaches disk
// 7. rename() the temp file to the final path (atomic on POSIX)
// 8. Open and fsync() the shard directory to persist the rename
// 9. Store the computed hash in *id_out
//
// HINTS - Useful syscalls and functions for this phase:
// - sprintf / snprintf  : formatting the header string
// - compute_hash        : hashing the combined header + data
// - object_exists       : checking for deduplication
// - mkdir               : creating the shard directory (use mode 0755)
// - open, write, close  : creating and writing to the temp file
//                         (Use O_CREAT | O_WRONLY | O_TRUNC, mode 0644)
// - fsync               : flushing the file descriptor to disk
// - rename              : atomically moving the temp file to the final path
//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: Determine type string
    const char *type_str;
    switch (type) {
        case OBJ_BLOB:   type_str = "blob";   break;
        case OBJ_TREE:   type_str = "tree";   break;
        case OBJ_COMMIT: type_str = "commit"; break;
        default: return -1;
    }
    
    // Step 2: Build header: "type len\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    
    // Step 3: Compute hash of full object (header + data)
    size_t total_size = header_len + 1 + len;
    
    // Allocate memory for full object (header + null byte + data)
    // Memory will be freed after writing to disk or on error
    void *full_object = malloc(total_size);
    if (!full_object) return -1;
    
    memcpy(full_object, header, header_len);
    ((char *)full_object)[header_len] = '\0';
    if (len > 0 && data != NULL) {
        memcpy((char *)full_object + header_len + 1, data, len);
    }
    
    ObjectID id;
    compute_hash(full_object, total_size, &id);
    
    // Step 4: Check if object already exists (deduplication)
    if (object_exists(&id)) {
        free(full_object);
        if (id_out) *id_out = id;
        return 0;
    }
    
    // Step 5: Create shard directory
    char path[512];
    object_path(&id, path, sizeof(path));
    
    // Extract directory path (everything before the last /)
    char dir_path[512];
    char *last_slash = strrchr(path, '/');
    if (last_slash) {
        size_t dir_len = last_slash - path;
        strncpy(dir_path, path, dir_len);
        dir_path[dir_len] = '\0';
    } else {
        free(full_object);
        return -1;
    }
    mkdir(dir_path, 0755);
    
    // Step 6: Write to temporary file
    char temp_path[520];
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", path);
    
    int fd = open(temp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full_object);
        return -1;
    }
    
    ssize_t written = write(fd, full_object, total_size);
    free(full_object);
    
    if (written != (ssize_t)total_size) {
        close(fd);
        unlink(temp_path);
        return -1;
    }
    
    // Step 7: fsync the file
    fsync(fd);
    close(fd);
    
    // Step 8: Atomic rename
    if (rename(temp_path, path) != 0) {
        unlink(temp_path);
        return -1;
    }
    
    // Step 9: Store hash
    if (id_out) *id_out = id;
    
    return 0;
}

// Read an object from the store.
//
// Steps:
// 1. Build the file path from the hash using object_path()
// 2. Open and read the entire file
// 3. Parse the header to extract the type string and size
// 4. Verify integrity: recompute the SHA-256 of the file contents
//    and compare to the expected hash (from *id). Return -1 if mismatch.
// 5. Set *type_out to the parsed ObjectType
// 6. Allocate a buffer, copy the data portion (after the \0), set *data_out and *len_out
//
// HINTS - Useful syscalls and functions for this phase:
// - object_path         : getting the target file path
// - fopen, fread, fseek : reading the file into memory
// - memchr              : safely finding the '\0' separating header and data
// - strncmp             : parsing the type string ("blob", "tree", "commit")
// - compute_hash        : re-hashing the read data for integrity verification
// - memcmp              : comparing the computed hash against the requested hash
// - malloc, memcpy      : allocating and returning the extracted data
//
// The caller is responsible for calling free(*data_out).
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Build file path
    char path[512];
    object_path(id, path, sizeof(path));
    
    // Step 2: Open and read the file
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    
    // Get file size
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return -1;
    }
    size_t file_size = st.st_size;
    
    // Read entire file
    void *buffer = malloc(file_size);
    if (!buffer) {
        close(fd);
        return -1;
    }
    
    ssize_t bytes_read = read(fd, buffer, file_size);
    close(fd);
    
    if (bytes_read != (ssize_t)file_size) {
        free(buffer);
        return -1;
    }
    
    // Step 3: Find the null byte separator
    char *null_pos = memchr(buffer, '\0', file_size);
    if (!null_pos) {
        free(buffer);
        return -1;
    }
    
    // Step 4: Parse header
    size_t header_len = null_pos - (char *)buffer;
    char *header = malloc(header_len + 1);
    memcpy(header, buffer, header_len);
    header[header_len] = '\0';
    
    // Extract type and size from header
    char type_str[16];
    size_t declared_size;
    if (sscanf(header, "%15s %zu", type_str, &declared_size) != 2) {
        free(header);
        free(buffer);
        return -1;
    }
    free(header);
    
    // Step 5: Determine object type
    if (strcmp(type_str, "blob") == 0) {
        if (type_out) *type_out = OBJ_BLOB;
    } else if (strcmp(type_str, "tree") == 0) {
        if (type_out) *type_out = OBJ_TREE;
    } else if (strcmp(type_str, "commit") == 0) {
        if (type_out) *type_out = OBJ_COMMIT;
    } else {
        free(buffer);
        return -1;
    }
    
    // Step 6: Verify size
    size_t data_size = file_size - header_len - 1;
    if (data_size != declared_size) {
        free(buffer);
        return -1;
    }
    
    // Step 7: Verify integrity (recompute hash)
    ObjectID computed_id;
    compute_hash(buffer, file_size, &computed_id);
    
    if (memcmp(&computed_id, id, sizeof(ObjectID)) != 0) {
        fprintf(stderr, "Error: Object integrity check failed - hash mismatch\n");
        free(buffer);
        return -1;  // Hash mismatch - corrupted!
    }
    
    // Step 8: Extract and return data
    if (len_out) *len_out = data_size;
    
    if (data_out) {
        *data_out = malloc(data_size);
        if (!*data_out) {
            free(buffer);
            return -1;
        }
        memcpy(*data_out, (char *)buffer + header_len + 1, data_size);
    }
    
    free(buffer);
    return 0;
}