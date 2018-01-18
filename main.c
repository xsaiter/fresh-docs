#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include <libpq-fe.h>

#include <curl/curl.h>

static void die(const char *message)
{
    perror(message);
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
        return NULL;
    }

    fread(res, len, 1, f);

    fclose(f);

    return res;
}

PGconn *open_or_die_connection(const char *conninfo)
{
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        die("failed to open connection");
    }
    return conn;
}

inline static const char *get_merge_sql()
{
    return read_text_file("merge.pgsql");
}

static char *get_conn_str()
{
    return "host=127.0.0.1 port=5432 dbname=venus user=isa password=1q2w3e connect_timeout=2";
}

static char *get_url(){
    return "http://";
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
    const char *merge_sql = get_merge_sql();
    if (!merge_sql) {
        die("failed to get merge sql");
    }

    char *conn_str = get_conn_str();
    PGconn *conn = open_or_die_connection(conn_str);
    return (EXIT_SUCCESS);
}