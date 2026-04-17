// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
// <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// Forward declaration from object.c
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1;
                    break;
                }
            }
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");
    return 0;
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

static int compare_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

int index_load(Index *index) {
    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) {
        // No index file yet — that's fine, start empty
        return 0;
    }

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        if (index->count >= MAX_INDEX_ENTRIES) break;

        // Strip newline
        line[strcspn(line, "\r\n")] = '\0';
        if (strlen(line) == 0) continue;

        IndexEntry *e = &index->entries[index->count];
        char hex[HASH_HEX_SIZE + 2];

        // Format: <mode-octal> <hex> <mtime> <size> <path>
        if (sscanf(line, "%o %64s %llu %llu %255s",
                   &e->mode, hex,
                   (unsigned long long *)&e->mtime_sec,
                   (unsigned long long *)&e->size,
                   e->path) != 5) {
            continue;
        }

        if (hex_to_hash(hex, &e->hash) != 0) continue;

        index->count++;
    }

    fclose(f);
    return 0;
}

int index_save(const Index *index) {
    // Sort entries by path
    Index sorted = *index;
    qsort(sorted.entries, sorted.count, sizeof(IndexEntry), compare_entries);

    // Write to temp file
    char tmp_path[256];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", INDEX_FILE);

    FILE *f = fopen(tmp_path, "w");
    if (!f) return -1;

    for (int i = 0; i < sorted.count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&sorted.entries[i].hash, hex);
        fprintf(f, "%o %s %llu %llu %s\n",
                sorted.entries[i].mode,
                hex,
                (unsigned long long)sorted.entries[i].mtime_sec,
                (unsigned long long)sorted.entries[i].size,
                sorted.entries[i].path);
    }

    fflush(f);
    fsync(fileno(f));
    fclose(f);

    return rename(tmp_path, INDEX_FILE);
}

int index_add(Index *index, const char *path) {
    // Read the file contents
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size < 0) { fclose(f); return -1; }

    uint8_t *contents = malloc(file_size + 1);
    if (!contents) { fclose(f); return -1; }

    size_t read_len = fread(contents, 1, file_size, f);
    fclose(f);

    // Write blob to object store
    ObjectID blob_id;
    if (object_write(OBJ_BLOB, contents, read_len, &blob_id) != 0) {
        free(contents);
        return -1;
    }
    free(contents);

    // Get file metadata
    struct stat st;
    if (lstat(path, &st) != 0) return -1;

    // Determine mode
    uint32_t mode;
    if (st.st_mode & S_IXUSR) mode = 0100755;
    else mode = 0100644;

    // Check if already in index
    IndexEntry *existing = index_find(index, path);
    if (existing) {
        // Update existing entry
        existing->hash = blob_id;
        existing->mode = mode;
        existing->mtime_sec = (uint64_t)st.st_mtime;
        existing->size = (uint64_t)st.st_size;
    } else {
        // Add new entry
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: index full\n");
            return -1;
        }
        IndexEntry *e = &index->entries[index->count++];
        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';
        e->hash = blob_id;
        e->mode = mode;
        e->mtime_sec = (uint64_t)st.st_mtime;
        e->size = (uint64_t)st.st_size;
    }

    return index_save(index);
}
// Phase 3: Loading logic
// Phase 3: Persistence
// Phase 3: Search
