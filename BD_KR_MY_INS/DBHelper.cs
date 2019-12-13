using System.Data.Odbc;

namespace BD_KR_MY_INS
{
    class DBHelper
    {
        private static string[] insInto = new string[] { "INSERT INTO pmib6703.rbd1 VALUES (nextval('pmib6703.rbd1_id_inc'),", "INSERT INTO pmib6703.rbd2 VALUES (nextval('pmib6703.rbd2_id_inc'),", "INSERT INTO pmib6703.rbd3 VALUES (nextval('pmib6703.rbd3_id_inc')," };

        public OdbcConnection connectToDB()
        {
            // Создаем объект подключения
            OdbcConnection conn = new OdbcConnection();
            // Задаем параметр подключения – имя ODBC-источника
            conn.ConnectionString = "Dsn=PostgreSQL31";
            // Подключаемся к БД
            conn.Open();
            return conn;
        }

    }
}
