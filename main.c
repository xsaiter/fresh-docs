#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include <libpq-fe.h>

#include <curl/curl.h>

#define LOG_ERR(...) fprintf(stderr, __VA_ARGS__)
#define LOG_INFO(...) printf(__VA_ARGS__)

typedef struct {
    char *db_conn_str;
    char *url;
} config_s;

config_s cfg;

void load_config()
{
    cfg.db_conn_str = "host=17.0.0.1 port=5432 dbname=venus user=isa password=1q2w3e connect_timeout=2";
    cfg.url = "http://";
}

static void die(PGconn *conn, const char *msg)
{
    if (conn) {
        PQfinish(conn);
    }

    if (msg) {
        perror(msg);
    }

    exit(EXIT_FAILURE);
}

const char *read_text_file(const char *filename)
{
    FILE *f = fopen(filename, "r");
    if (!f) {
        return NULL;
    }

    fseek(f, 0L, SEEK_END);
    long len = ftell(f);
    rewind(f);

    char *res = malloc(len);
    if (!res) {
        fclose(f);
        return NULL;
    }

    fread(res, 1, len, f);

    fclose(f);

    return res;
}

PGconn *open_or_die_connection(const char *conninfo)
{
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        LOG_ERR("failed to open connection: %s", PQerrorMessage(conn));
        die(conn, NULL);
    }
    return conn;
}

inline static const char *get_merge_sql()
{
    return read_text_file("merge.pgsql");
}

size_t write_file(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    return fwrite(ptr, size, nmemb, stream);
}

int download_file(const char *url, const char *filename)
{
    return -1;
    CURL *curl = curl_easy_init();
    if (!curl) {
        return -1;
    }

    FILE *f = fopen(filename, "wb");
    if (!f) {
        die(NULL, "failed to open file");
    }

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_file);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, f);

    CURLcode code = curl_easy_perform(curl);
    if (code != CURLE_OK) {
        curl_easy_cleanup(curl);
        fclose(f);
        return -1;
    }

    curl_easy_cleanup(curl);
    fclose(f);

    return 0;
}

int main(int argc, char** argv)
{
    load_config();

    int res = download_file(cfg.url, "data");
    if (!res) {
        die(NULL, "failed to download file");
    }

    const char *merge_sql = get_merge_sql();
    if (!merge_sql) {
        die(NULL, "failed to get merge sql");
    }

    PGconn *conn = open_or_die_connection(cfg.db_conn_str);

    return (EXIT_SUCCESS);
}