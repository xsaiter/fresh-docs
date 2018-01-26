#define _GNU_SOURCE

#define FD_TEST

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include "libpq-fe.h"
#include "curl/curl.h"
#include "bzlib.h"

#define LOG_ERR(...) fprintf(stderr, __VA_ARGS__)
#define LOG_INFO(...) printf(__VA_ARGS__)

typedef struct {
    char *db_conn_str;
    char *url;
    char *download_filename;
    char *decompress_filename;
} config_s;

config_s cfg;

void load_config()
{
    cfg.db_conn_str = "host=127.0.0.1 port=5432 dbname=venus user=isa password=1q2w3e connect_timeout=2";
    cfg.url = "https://guvm.mvd.ru/upload/expired-passports/list_of_expired_passports.csv.bz2";

#ifdef FD_TEST
    cfg.download_filename = "data/test_data.txt.bz2";
    cfg.decompress_filename = "data/test_data.txt";
#elif
    cfg.download_filename = "data/download.csv.bz2";
    cfg.decompress_filename = "data/download.csv";
#endif
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

static PGconn *conn_open_or_die(const char *conninfo)
{
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        LOG_ERR("failed to open connection: %s", PQerrorMessage(conn));
        die(conn, NULL);
    }

    int socket_fd = PQsocket(conn);
    if (socket_fd < 0) {
        LOG_ERR("socket: %s", PQerrorMessage(conn));
        die(conn, NULL);
    }

    struct timeval timeout = {90000, 0};

    int setopt_recv = setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,
            (char *) &timeout, sizeof (timeout));
    if (setopt_recv < 0) {
        LOG_ERR("setopt_recv: %s", PQerrorMessage(conn));
        die(conn, NULL);
    }

    int setopt_send = setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO,
            (char *) &timeout, sizeof (timeout));
    if (setopt_send < 0) {
        LOG_ERR("setopt_send: %s", PQerrorMessage(conn));
        die(conn, NULL);
    }

    return conn;
}

inline static void conn_close(PGconn *conn)
{
    PQfinish(conn);
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

void create_tank(PGconn *conn)
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

void fill_tank(PGconn *conn, char *filename)
{
    FILE *f = fopen(filename, "r");
    if (f == NULL) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    int r = 0;
    const char *query = "copy tank (raw) from stdin;";

    PGresult * res = NULL;
    char * line = NULL;
    size_t len = 0;
    ssize_t read;
    char *errormsg = NULL;

    while ((read = getline(&line, &len, f)) != -1) {
        res = PQexec(conn, query);
        r = PQputCopyData(conn, line, read);
        if (r != 1) {
            cleanup_and_die(conn, res);
        }

        r = PQputCopyEnd(conn, errormsg);

        if (r != 1) {
            cleanup_and_die(conn, res);
        }
    }

    fclose(f);
    if (line) {
        free(line);
    }

    res = PQexec(conn, "commit;");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        cleanup_and_die(conn, res);
    }
}

static const char *read_text_file(const char *filename)
{
    FILE *fp = fopen(filename, "r");
    if (!fp) {
       fprintf(stderr, "failed to open file %s", filename);
       perror(filename);
       return NULL;
    }

    fseek(fp, 0L, SEEK_END);
    long len = ftell(fp);
    rewind(fp);

    char *res = malloc(len);
    if (!res) {
        fclose(fp);        
        return NULL;
    }

    fread(res, 1, len, fp);

    fclose(fp);

    return res;
}

void merge_tank(PGconn *conn)
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

void decompress_bz2(const char *fn_r, const char *fn_w)
{
    FILE *fp_w = fopen(fn_w, "wb");
    if (!fp_w) {
        perror("open");
        exit(1);
    }

    BZFILE *fp_r = BZ2_bzopen(fn_r, "rb");
    if (!fp_r) {
        fclose(fp_w);
        perror("bzopen");
        exit(1);
    }

    int len;
    const int n = 0x1000;
    char s[n];

    while ((len = BZ2_bzread(fp_r, s, n)) > 0) {
        fwrite(s, 1, len, fp_w);
    }

    BZ2_bzclose(fp_r);
    fclose(fp_w);
}

static size_t write_file(void *ptr, size_t size, size_t nmemb, FILE *fp)
{
    return fwrite(ptr, size, nmemb, fp);
}

int download_file(const char *url, const char *filename)
{
    CURL *curl = curl_easy_init();
    if (!curl) {
        return -1;
    }

    FILE *fp = fopen(filename, "wb");
    if (!fp) {
        curl_easy_cleanup(curl);
        die(NULL, "failed to open file");
    }

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_file);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 500L);

    CURLcode code = curl_easy_perform(curl);
    if (code != CURLE_OK) {
        curl_easy_cleanup(curl);
        fclose(fp);
        return -1;
    }

    fclose(fp);

    curl_easy_cleanup(curl);

    return 0;
}

static void test_decompress_bz2()
{
    decompress_bz2("data/test_data.txt.bz2", "data/out.txt");
}

int main(int argc, char** argv)
{
    load_config();

#ifndef FD_TEST
    download_file(cfg.url, cfg.download_filename);
#endif
    decompress_bz2(cfg.download_filename, cfg.decompress_filename);

    PGconn *conn = conn_open_or_die(cfg.db_conn_str);

    create_tank(conn);

    fill_tank(conn, cfg.decompress_filename);

    merge_tank(conn);

    conn_close(conn);

    return (EXIT_SUCCESS);
}