#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include <libpq-fe.h>
#include <curl/curl.h>
#include <bzlib.h>

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

static PGconn *conn_open_or_die(const char *conninfo)
{
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        LOG_ERR("failed to open connection: %s", PQerrorMessage(conn));
        die(conn, NULL);
    }
    return conn;
}

inline static void conn_close(PGconn *conn)
{
    PQfinish(conn);
}

size_t write_file(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    return fwrite(ptr, size, nmemb, stream);
}

int download_file(const char *url, const char *filename)
{
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

static void cleanup_and_die(PGconn *conn, PGresult *res)
{
    fprintf(stderr, "%s\n", PQerrorMessage(conn));

    if (res) {
        PQclear(res);
    }

    if (conn) {
        PQfinish(conn);
    }

    exit(EXIT_FAILURE);
}

void tank_create(PGconn *conn)
{
    PGresult *res = PQexec(conn,
            "create temp table tank "
            "(id bigserial, "
            "serie character varying(15) null, "
            "number character varying(15) null, "
            "raw character varying(30) not null);");

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        cleanup_and_die(conn, res);
    }

    PQclear(res);
}

void tank_fill(PGconn *conn)
{
    char *err_msg = NULL;

    char buf[] = "test";

    PGresult *res = PQexec(conn, "copy tank (raw) from stdin;");
    int copy_res = PQputCopyData(conn, buf, strlen(buf));
    if (copy_res != 1) {
        cleanup_and_die(conn, res);
    }
    copy_res = PQputCopyEnd(conn, err_msg);
    if (copy_res != 1) {
        cleanup_and_die(conn, res);
    }

    res = PQexec(conn, "commit;");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        cleanup_and_die(conn, res);
    }
}

void tank_merge(PGconn *conn)
{
    const char *merge_sql = read_text_file("merge.pgsql");
    if (!merge_sql) {
        die(conn, "failed to get merge sql");
    }

    PGresult *res = PQexec(conn, merge_sql);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        cleanup_and_die(conn, res);
    }

    PQclear(res);
}

int main(int argc, char** argv)
{
    load_config();

    const char *fname = "data";

    int res = download_file(cfg.url, fname);
    if (!res) {
        die(NULL, "failed to download file");
    }

    PGconn *conn = conn_open_or_die(cfg.db_conn_str);

    tank_create(conn);
    tank_fill(conn);
    tank_merge(conn);

    conn_close(conn);

    return (EXIT_SUCCESS);
}