// object.c — Content-addressable object store
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

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
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
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

// ─── IMPLEMENTED ──────────────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: Build header
    const char *type_str;
    if      (type == OBJ_BLOB)   type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    // header_len does NOT include the null byte; the null byte is part of the format
    size_t total_len = header_len + 1 + len; // +1 for the '\0' separator

    uint8_t *full_obj = malloc(total_len);
    if (!full_obj) return -1;

    memcpy(full_obj, header, header_len);
    full_obj[header_len] = '\0';              // the separator null byte
    memcpy(full_obj + header_len + 1, data, len);

    // Step 2: Compute hash of full object
    ObjectID id;
    compute_hash(full_obj, total_len, &id);

    // Step 3: Deduplication
    if (object_exists(&id)) {
        *id_out = id;
        free(full_obj);
        return 0;
    }

    // Step 4: Create shard directory
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755);  // ignore error if already exists

    // Step 5: Build final path and temp path
    char final_path[512];
    object_path(&id, final_path, sizeof(final_path));
    char tmp_path[520];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", final_path);

    // Step 6: Write to temp file
    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) { free(full_obj); return -1; }

    ssize_t written = write(fd, full_obj, total_len);
    free(full_obj);
    if (written < 0 || (size_t)written != total_len) {
        close(fd);
        return -1;
    }

    // Step 7: fsync the temp file
    fsync(fd);
    close(fd);

    // Step 8: Atomic rename
    if (rename(tmp_path, final_path) != 0) return -1;

    // Step 9: fsync the shard directory
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    *id_out = id;
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Build file path
    char path[512];
    object_path(id, path, sizeof(path));

    // Step 2: Open and read entire file
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size <= 0) { fclose(f); return -1; }

    uint8_t *buf = malloc(file_size);
    if (!buf) { fclose(f); return -1; }

    if (fread(buf, 1, file_size, f) != (size_t)file_size) {
        fclose(f); free(buf); return -1;
    }
    fclose(f);

    // Step 3: Parse header — find null byte
    uint8_t *null_pos = memchr(buf, '\0', file_size);
    if (!null_pos) { free(buf); return -1; }

    size_t header_len = null_pos - buf;
    char header[64];
    if (header_len >= sizeof(header)) { free(buf); return -1; }
    memcpy(header, buf, header_len);
    header[header_len] = '\0';

    // Parse type and size from header
    char type_str[16];
    size_t data_size;
    if (sscanf(header, "%15s %zu", type_str, &data_size) != 2) {
        free(buf); return -1;
    }

    if      (strcmp(type_str, "blob")   == 0) *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree")   == 0) *type_out = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0) *type_out = OBJ_COMMIT;
    else { free(buf); return -1; }

    // Step 4: Verify integrity
    ObjectID computed;
    compute_hash(buf, file_size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(buf); return -1;
    }

    // Step 5: Return data portion
    uint8_t *data_start = null_pos + 1;
    size_t actual_data_len = file_size - header_len - 1;

    *data_out = malloc(actual_data_len + 1);
    if (!*data_out) { free(buf); return -1; }
    memcpy(*data_out, data_start, actual_data_len);
    ((uint8_t *)*data_out)[actual_data_len] = '\0';
    *len_out = actual_data_len;

    free(buf);
    return 0;
}
