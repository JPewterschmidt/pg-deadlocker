#include <stdio.h>
#include <libpq/libpq-fs.h>
#include <libpq-fe.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>

const char* conninfo = "host=localhost port=5432 dbname=postgres password=shit";

thread_local const char* thrname;
thread_local bool err_occured = false;

sem_t sem_trx1;
sem_t sem_trx2;
sem_t sem_trx3;
pthread_barrier_t b_exit;


enum pg_lock_type {
    SHARED, EXCLUSIVE
};

void begin_trx(PGconn* conn)
{
    PGresult* res = PQexec(conn, "begin;");
    printf("%s: begin trx\n", thrname);
    PQclear(res);
}

void abort_trx(PGconn* conn)
{
    PGresult* res = PQexec(conn, "abort;");
    printf("%s: abort trx\n", thrname);
    PQclear(res);
}

void commit_trx(PGconn* conn)
{
    if (err_occured) {
        err_occured = false;
        abort_trx(conn);
        return;
    }

    PGresult* res = PQexec(conn, "commit;");
    printf("%s: commit trx\n", thrname);
    PQclear(res);
}

void execute_sql(PGconn* conn, const char* sql, sem_t* post_after_send)
{
    PGresult* res = NULL;

    if (!PQsendQuery(conn, sql)) {
        printf("%s: \t%s\n", thrname, PQerrorMessage(conn));
    }
    
    usleep(500);
    if (post_after_send)
        sem_post(post_after_send);
    
    while ((res = PQgetResult(conn))) {
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            printf("%s: \t[\n%s]\n\n", thrname, PQerrorMessage(conn));
            err_occured = true;
        }
        PQclear(res);
    }
    if (err_occured) {
        abort_trx(conn);
        pthread_barrier_wait(&b_exit);
        pthread_exit(NULL);
    }
}

void init_table(PGconn* conn, const char* t)
{
    char sql[512] = {0};
    PGresult* res;
    const char* sqlfmt = 
        "CREATE TABLE IF NOT EXISTS %s ("
        "    id SERIAL PRIMARY KEY,"
        "    col TEXT NOT NULL"
        ");"
        "TRUNCATE %s;"
        "INSERT INTO %s (col)"
        "SELECT md5(random()::text)"
        "FROM generate_series(1, 10);";

    snprintf(sql, sizeof(sql), sqlfmt, t, t, t, t);
    res = PQexec(conn, sql);
    puts(PQerrorMessage(conn));
    PQclear(res);
}

void lock_table(PGconn* conn, const char* table, enum pg_lock_type t, sem_t* post_after_send)
{
    char sql[128] = {0};
    snprintf(sql, sizeof(sql), "lock table %s in %s mode", table, t == SHARED ? "share" : "exclusive");
    printf("%s: \tbefore \t[%s]\n", thrname, sql);
    execute_sql(conn, sql, post_after_send);
    printf("%s: \tafter \t[%s]\n", thrname, sql);
}

void* trx1(void* arg)
{
    PGconn* conn = (PGconn*)arg;
    thrname = __func__;

    sem_wait(&sem_trx1);
    // ===================================================
    begin_trx(conn);
    sem_post(&sem_trx2);
    
    sem_wait(&sem_trx1);
    lock_table(conn, "t1", EXCLUSIVE, NULL);
    sem_post(&sem_trx2);

    sem_wait(&sem_trx1);
    lock_table(conn, "t2", SHARED, NULL);

    pthread_barrier_wait(&b_exit);
    commit_trx(conn);
    // ===================================================
    printf("%s: gone\n", thrname);
}

void* trx2(void* arg)
{
    PGconn* conn = (PGconn*)arg;
    thrname = __func__;

    sem_wait(&sem_trx2);
    // ===================================================
    begin_trx(conn);
    sem_post(&sem_trx3);
    
    sem_wait(&sem_trx2);
    lock_table(conn, "t1", EXCLUSIVE, &sem_trx3);
    
    pthread_barrier_wait(&b_exit);
    commit_trx(conn);
    // ===================================================
    printf("%s: gone\n", thrname);
}
    
void* trx3(void* arg)
{
    PGconn* conn = (PGconn*)arg;
    thrname = __func__;
    sem_wait(&sem_trx3);
    // ===================================================
    begin_trx(conn);
    sem_post(&sem_trx1);
       
    sem_wait(&sem_trx3);
    lock_table(conn, "t2", EXCLUSIVE, NULL);
    lock_table(conn, "t1", SHARED, &sem_trx1);

    pthread_barrier_wait(&b_exit);
    commit_trx(conn);
    // ===================================================
    printf("%s: gone\n", thrname);
}

int main(int argc, char** argv) 
{ 
    pthread_t thr[3];
    PGconn* conn1 = PQconnectdb(conninfo);
    PGconn* conn2 = PQconnectdb(conninfo);
    PGconn* conn3 = PQconnectdb(conninfo);

    assert(conn1);
    assert(conn2);
    assert(conn3);
    
    init_table(conn1, "t1");
    init_table(conn1, "t2");

    sem_init(&sem_trx1, 0, 1);
    sem_init(&sem_trx2, 0, 0);
    sem_init(&sem_trx3, 0, 0);
    pthread_barrier_init(&b_exit, NULL, 3);

    pthread_create(&thr[0], NULL, trx1, (void*)conn1);
    pthread_create(&thr[1], NULL, trx2, (void*)conn2);
    pthread_create(&thr[2], NULL, trx3, (void*)conn3);
    
    for (size_t i = 0; i < 3; ++i) {
        pthread_join(thr[i], NULL);
    }

    PQfinish(conn1);
    PQfinish(conn2);
    PQfinish(conn3);

    sem_destroy(&sem_trx1);
    sem_destroy(&sem_trx2);
    sem_destroy(&sem_trx3);
    
    pthread_barrier_destroy(&b_exit);

    return 0;
}
