#include <stdio.h>
#include <libpq/libpq-fs.h>

const char* conninfo = "host=localhost port=5432 dbnamne=postgres password=shit";

struct pg_res_t {
    PGconn* pr_conn;
    PGresult* pr_result;
};

void execute_sql(struct PQconn* conn, const char* sql)
{
    PGresult* res = PQexec(conn, sql);
    PQclear(res);
}

void init_conn(struct pg_res_t* res)
{
    memset(res, 0, sizeof(*res));
    res->pr_conn = PQconnectdb(conninfo);
}

void dest_conn(struct pg_res_t* res)
{
    if (res->pr_result)
        PQclear(res->pr_result);

    PQfinish(res->pr_conn);
}

void init_table(struct pg_res_t* res)
{
    const char* sql = 
        "DROP TABLE t1; "
        "CREATE TABLE IF NOT EXISTS t1 ("
        "    id SERIAL PRIMARY KEY,"
        "    col TEXT NOT NULL"
        ");"
        "TRUNCATE t1;"
        "INSERT INTO t1 (col)"
        "SELECT md5(random()::text)"
        "FROM generate_series(1, 10);";

    res->pr_result = PQexec(res->pr_conn, sql);
    PQclear(res->pr_result);
}

void begin_trx(PQconn* conn)
{
    PQresult* res = PQexec(conn, "begin;");
    PQclear(res);
}

void abort_trx(PQconn* conn)
{
    PQresult* res = PQexec(conn, "abort;");
    PQclear(res);
}

void commit_trx(PQconn* conn)
{
    PQresult* res = PQexec(conn, "commit;");
    PQclear(res);
}

void* trx1(void* arg)
{
    PQconn* conn = (PQconn*)arg;
    begin_trx(conn);
    execute_sql(conn, "lock table t1 in share mode");       

    wait_for_trx3_acquire_share();

    execute_sql(conn, "lock table t1 in share mode");       

    commit_trx(conn);
}

void* trx2(void* arg)
{
    PQconn* conn = (PQconn*)arg;
    begin_trx(conn);
    execute_sql(conn, "lock table t1 in exclusive mode");      


    commit_trx(conn);
}

void* trx3(void* arg)
{
    PQconn* conn = (PQconn*)arg;
    begin_trx(conn);
       
    wait_for_trx2_hold_exclusive();

    execute_sql(conn, "lock table t1 in share mode");      
    

    commit_trx(conn);
}


int main(int argc, char** argv) 
{ 
    struct pg_res_t conn1;
    struct pg_res_t conn2;
    struct pg_res_t conn3;
    
    init_conn(&conn1);
    init_conn(&conn2);
    init_conn(&conn3);

    init_table(&conn1);
    
    
    

    dest_conn(&conn1);
    dest_conn(&conn2);
    dest_conn(&conn3);
    
    return 0;
}
