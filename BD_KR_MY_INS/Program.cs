using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Data.SqlClient;

namespace BD_KR_MY_INS
{
    class Program
    {
        private static string[] tableList = new string[] { "pmib6703.rbd1", "pmib6703.rbd2", "pmib6703.rbd3" };

        private static string[] insInto = new string[] { "INSERT INTO pmib6703.rbd1 VALUES (nextval('pmib6703.rbd1_id_inc'),", "INSERT INTO pmib6703.rbd2 VALUES (nextval('pmib6703.rbd2_id_inc'),", "INSERT INTO pmib6703.rbd3 VALUES (nextval('pmib6703.rbd3_id_inc')," };

        private string[] randProduct = new string[] { "Ryzen 3 2300", "GTX 1650 Ti", "Vega 56", "Celeron G3200", "Xeon 2640V3", "Xeon 2689", "Samsung DDR4 8Gb(B-Die)" };

        private static string[] vals = new string[] {"'Ryzen 5 2600','Начальная вставка',current_timestamp)",
        "'Ryzen 7 2700X','Начальная вставка',current_timestamp)",
        "'Ryzen 7 2700','Начальная вставка',current_timestamp)",
        "'Core i5 8600','Начальная вставка',current_timestamp)",
        "'Radeon RX580','Начальная вставка',current_timestamp)",
        "'GeForce RTX 2080 Super','Начальная вставка',current_timestamp)",
        "'GeForce RTX 2060','Начальная вставка',current_timestamp)",
        "'Radeon Vega 64','Начальная вставка',current_timestamp)",
        "'Aerocool KCAS 600','Начальная вставка',current_timestamp)",
        "'Aerocool KCAS 700','Начальная вставка',current_timestamp)",
        "'Pentium G4560','Начальная вставка',current_timestamp)",
        "'GAMMAXX 300','Начальная вставка',current_timestamp)",
        "'FX 8300','Начальная вставка',current_timestamp)",
        "'FX 6300','Начальная вставка',current_timestamp)",
        "'Core i7 2600K','Начальная вставка',current_timestamp)"};

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

        private void initData()
        {
            OdbcConnection conn = connectToDB();
            for (int i = 0; i < insInto.Length; i++)
            {
                for (int j = 0; j < vals.Length; j++)
                {
                    OdbcCommand cmd = new OdbcCommand(insInto[i] + vals[j], conn);
                    OdbcTransaction tx = null;

                    try
                    {
                        tx = conn.BeginTransaction();
                        cmd.Transaction = tx;
                        Console.WriteLine(cmd.ExecuteNonQuery().ToString());
                        tx.Commit();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        tx.Rollback();
                    }
                    System.Threading.Thread.Sleep(6);
                }

            }
            conn.Close();
        }

        private ArrayList getCurrentTableLog(OdbcConnection conn, int randTable)
        {
            OdbcCommand selectCmd = new OdbcCommand("SELECT * FROM " + tableList[randTable] + " WHERE n_izd = (SELECT min(n_izd) FROM " + tableList[randTable] + ")", conn);
            OdbcDataReader selectReader = selectCmd.ExecuteReader();
            selectReader.Read();
            ArrayList tmp = new ArrayList();
            tmp.Add(selectReader.GetInt32(0));
            tmp.Add(selectReader.GetString(1));
            tmp.Add(selectReader.GetString(2));
            tmp.Add(selectReader.GetDateTime(3));
            return tmp;

        }

        private void imitate()
        {
            Random random = new Random();
            ArrayList before = new ArrayList();
            ArrayList after = new ArrayList();
            OdbcConnection conn = connectToDB();
            int i = random.Next(3);
            i = 0; //Debug
            int randTable = random.Next(3);

            OdbcCommand cmd = null;
            switch (i)
            {
                //update
                case 0:
                    {
                        before = getCurrentTableLog(conn, randTable);
                        string sqlCmd = "UPDATE " + tableList[randTable] + " SET name = ?, date = current_timestamp, o_type = 'Обновление " + tableList[randTable] + "' WHERE n_izd = (SELECT min(n_izd) FROM " + tableList[randTable] + ")";
                        cmd = new OdbcCommand(sqlCmd, conn);
                        List<OdbcParameter> paramList = new List<OdbcParameter>();
                        OdbcParameter nameParam = new OdbcParameter();
                        nameParam.ParameterName = "@name";
                        nameParam.OdbcType = OdbcType.Text;
                        nameParam.Value = randProduct[random.Next(randProduct.Length)]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(nameParam);

                        //OdbcParameter nizdParam = new OdbcParameter();
                        //nizdParam.ParameterName = "@nizd";
                        //nizdParam.OdbcType = OdbcType.Int;
                        //nizdParam.Value = 1; //Выбираем случаайное значение из списка новых продуктов
                        //cmd.Parameters.Add(nizdParam);

                        break;
                    }
                default: break;
            }
            
                    OdbcTransaction tx = null;

                    try
                    {
                        tx = conn.BeginTransaction();
                        cmd.Transaction = tx;
                        Console.WriteLine(cmd.ExecuteNonQuery().ToString());
                        tx.Commit();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                        tx.Rollback();
                    }
                    System.Threading.Thread.Sleep(6);
            after = getCurrentTableLog(conn, randTable);
            conn.Close();

        }

        static void Main(string[] args)
        {
            Program a = new Program();
            a.imitate();
            //a.initData();
        }
    }
}
