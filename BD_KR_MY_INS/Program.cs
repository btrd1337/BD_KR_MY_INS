using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Odbc;

namespace BD_KR_MY_INS
{
    class Program
    {
        private static string[] tableList = new string[] { "pmib6703.rbd1", "pmib6703.rbd2", "pmib6703.rbd3", "pmib6703.cbd" };

        private static string[] insInto = new string[] { "INSERT INTO pmib6703.rbd1 VALUES (nextval('pmib6703.rbd1_id_inc'),", "INSERT INTO pmib6703.rbd2 VALUES (nextval('pmib6703.rbd2_id_inc'),", "INSERT INTO pmib6703.rbd3 VALUES (nextval('pmib6703.rbd3_id_inc'),", "INSERT INTO pmib6703.cbd VALUES (nextval('pmib6703.cbd_id_inc')," };

        private string[] randProduct = new string[] { "Ryzen 3 2300", "GTX 1650 Ti", "Vega 56", "Celeron G3200", "Xeon 2640V3", "Xeon 2689", "Samsung DDR4 8Gb(B-Die)" };

        private static string[] vals = new string[] {"'Ryzen 5 2600'",
        "'Ryzen 7 2700X'",
        "'Ryzen 7 2700'",
        "'Core i5 8600'",
        "'Radeon RX580'",
        "'GeForce RTX 2080 Super'",
        "'GeForce RTX 2060'",
        "'Radeon Vega 64'",
        "'Aerocool KCAS 600'",
        "'Aerocool KCAS 700'",
        "'Pentium G4560'",
        "'GAMMAXX 300'",
        "'FX 8300'",
        "'FX 6300'",
        "'Core i7 2600K'"};

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

        private int truncTable(OdbcConnection conn, string table)
        {
            OdbcCommand selectCmd = new OdbcCommand("TRUNCATE TABLE " + table, conn);
            OdbcTransaction tx = null;
            try
            {
                tx = conn.BeginTransaction();
                selectCmd.Transaction = tx;
                int tmp = selectCmd.ExecuteNonQuery();
                tx.Commit();
                return tmp;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                tx.Rollback();
                return -1;
            }
        }

        private void resetSeq(OdbcConnection conn, string seq)
        {
            OdbcCommand selectCmd = new OdbcCommand("ALTER SEQUENCE " + seq + " RESTART", conn);
            OdbcTransaction tx = null;
            try
            {
                tx = conn.BeginTransaction();
                selectCmd.Transaction = tx;
                int tmp = selectCmd.ExecuteNonQuery();
                tx.Commit();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                tx.Rollback();
            }
        }



        private void initData()
        {
            OdbcConnection conn = connectToDB();
            truncTable(conn, "pmib6703.log");
            for (int i = 0; i < tableList.Length; i++)
            {
                truncTable(conn, tableList[i]);
                resetSeq(conn, tableList[i] + "_id_inc");

            }

            for (int j = 0; j < vals.Length; j++)
            {
                OdbcTransaction tx = null;

                try
                {
                    tx = conn.BeginTransaction();
                    for (int i = 0; i < tableList.Length; i++)
                    {
                        int n_izd = getCounter(i);
                        OdbcCommand tmp = new OdbcCommand("INSERT INTO "+ tableList[i] + " VALUES (?," + vals[j] + ",'Начальная вставка', current_timestamp); INSERT INTO pmib6703.log VALUES (current_timestamp, '"+ tableList[i] +"', NULL, NULL, NULL, NULL, ?, "+vals[j]+ ",'Начальная вставка', current_timestamp);", conn);

                        OdbcParameter nizdParam = new OdbcParameter();
                        nizdParam.ParameterName = "@nizd";
                        nizdParam.OdbcType = OdbcType.Int;
                        nizdParam.Value = n_izd; //Выбираем случаайное значение из списка новых продуктов
                        tmp.Parameters.Add(nizdParam);

                        OdbcParameter nnizdParam = new OdbcParameter();
                        nnizdParam.ParameterName = "@nnizd";
                        nnizdParam.OdbcType = OdbcType.Int;
                        nnizdParam.Value = n_izd; //Выбираем случаайное значение из списка новых продуктов
                        tmp.Parameters.Add(nnizdParam);


                        tmp.Transaction = tx;
                        Console.WriteLine(tmp.ExecuteNonQuery().ToString());
                    }
                    //OdbcCommand logInto = new OdbcCommand("INSERT INTO pmib6703.log");
                    //cmd.Transaction = tx;

                    tx.Commit();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    tx.Rollback();
                }
            }

            conn.Close();
        }

        private ArrayList getCurrentTableLog(OdbcConnection conn, int randTable, string min)
        {
            OdbcCommand selectCmd = new OdbcCommand("SELECT * FROM " + tableList[randTable] + " WHERE n_izd = (SELECT " + min + "(n_izd) FROM " + tableList[randTable] + ")", conn);

            OdbcTransaction tx = null;

            try
            {
                OdbcDataReader selectReader = selectCmd.ExecuteReader();
                tx = conn.BeginTransaction();
                selectCmd.Transaction = tx;
                selectReader.Read();
                ArrayList tmp = new ArrayList();
                tmp.Add(selectReader.GetInt32(0));
                tmp.Add(selectReader.GetString(1));
                tmp.Add(selectReader.GetString(2));
                tmp.Add(selectReader.GetDateTime(3));
                tx.Commit();
                return tmp;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                tx.Rollback();
            }

            return null;

        }

        private int getCounter(int randTable)
        {
            OdbcConnection conn = connectToDB();
            OdbcCommand selectCmd = new OdbcCommand("SELECT nextval('" + tableList[randTable] + "_id_inc')", conn);
            OdbcTransaction tx = null;
            try
            {
                tx = conn.BeginTransaction();
                selectCmd.Transaction = tx;
                OdbcDataReader selectReader = selectCmd.ExecuteReader();
                selectReader.Read();
                tx.Commit();
                return selectReader.GetInt32(0);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                tx.Rollback();
            }
            conn.Close();
            return -1;

        }

        private void imitate()
        {
            string[] opList = { "update ", "insert ", "delete " };

            Random random = new Random();
            ArrayList before = new ArrayList();
            ArrayList after = new ArrayList();
            OdbcConnection conn = connectToDB();
            int i = random.Next(3);
            //Debug
            int randTable = random.Next(tableList.Length-1);

            OdbcCommand cmd = null;
            switch (i)
            {
                //update
                case 0:
                    {
                        before = getCurrentTableLog(conn, randTable, "min");
                        string sqlCmd = "UPDATE " + tableList[randTable] + " SET name = ?, date = current_timestamp, o_type = 'Обновление " + tableList[randTable] + "' WHERE n_izd = (SELECT min(n_izd) FROM " + tableList[randTable] + "); INSERT INTO pmib6703.log VALUES(current_timestamp, '" + tableList[randTable] + "', ?,?,?,?,?,?,?,current_timestamp)";
                        cmd = new OdbcCommand(sqlCmd, conn);

                        OdbcParameter nameParam = new OdbcParameter();
                        nameParam.ParameterName = "@name";
                        nameParam.OdbcType = OdbcType.Text;
                        nameParam.Value = randProduct[random.Next(randProduct.Length)]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(nameParam);

                        //OdbcParameter dateParam = new OdbcParameter();
                        //dateParam.ParameterName = "@name";
                        //dateParam.OdbcType = OdbcType.DateTime;
                        //dateParam.Value = DateTime.Now.AddHours(-1).AddMinutes(-1).AddSeconds(-15); //Выбираем случаайное значение из списка новых продуктов
                        //cmd.Parameters.Add(dateParam);

                        OdbcParameter oldnizdParam = new OdbcParameter();
                        oldnizdParam.ParameterName = "@onizd";
                        oldnizdParam.OdbcType = OdbcType.Int;
                        oldnizdParam.Value = before[0]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldnizdParam);

                        OdbcParameter oldName = new OdbcParameter();
                        oldName.ParameterName = "@oname";
                        oldName.OdbcType = OdbcType.Text;
                        oldName.Value = before[1]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldName);

                        OdbcParameter oldOType = new OdbcParameter();
                        oldOType.ParameterName = "@oname";
                        oldOType.OdbcType = OdbcType.Text;
                        oldOType.Value = before[2]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldOType);

                        OdbcParameter oldDate = new OdbcParameter();
                        oldDate.ParameterName = "@odate";
                        oldDate.OdbcType = OdbcType.DateTime;
                        oldDate.Value = before[3]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldDate);


                        OdbcParameter newnizdParam = new OdbcParameter();
                        newnizdParam.ParameterName = "@nnizd";
                        newnizdParam.OdbcType = OdbcType.Int;
                        newnizdParam.Value = before[0]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newnizdParam);

                        OdbcParameter newName = new OdbcParameter();
                        newName.ParameterName = "@oname";
                        newName.OdbcType = OdbcType.Text;
                        newName.Value = nameParam.Value; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newName);

                        OdbcParameter newOType = new OdbcParameter();
                        newOType.ParameterName = "@oname";
                        newOType.OdbcType = OdbcType.Text;
                        newOType.Value = "Обновление " + tableList[randTable]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newOType);

                        //OdbcParameter newDate = new OdbcParameter();
                        //newDate.ParameterName = "@odate";
                        //newDate.OdbcType = OdbcType.DateTime;
                        //newDate.Value = dateParam.Value; //Выбираем случаайное значение из списка новых продуктов
                        //cmd.Parameters.Add(newDate);

                        //OdbcParameter nizdParam = new OdbcParameter();
                        //nizdParam.ParameterName = "@nizd";
                        //nizdParam.OdbcType = OdbcType.Int;
                        //nizdParam.Value = 1; //Выбираем случаайное значение из списка новых продуктов
                        //cmd.Parameters.Add(nizdParam);

                        break;
                    }
                //insert
                case 1:
                    {
                        int n_izd = getCounter(randTable);

                        string sqlCmd = "INSERT INTO " + tableList[randTable] + " VALUES (?, ?, 'Вставка " + tableList[randTable] + "', current_timestamp); INSERT INTO pmib6703.log VALUES(current_timestamp, '" + tableList[randTable] + "', NULL,NULL,NULL,NULL,?,?,?,current_timestamp)";
                        cmd = new OdbcCommand(sqlCmd, conn);

                        OdbcParameter nizdParam = new OdbcParameter();
                        nizdParam.ParameterName = "@nizd";
                        nizdParam.OdbcType = OdbcType.Int;
                        nizdParam.Value = n_izd; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(nizdParam);

                        OdbcParameter nameParam = new OdbcParameter();
                        nameParam.ParameterName = "@name";
                        nameParam.OdbcType = OdbcType.Text;
                        nameParam.Value = randProduct[random.Next(randProduct.Length)]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(nameParam);

                        //OdbcParameter dateParam = new OdbcParameter();
                        //dateParam.ParameterName = "@name";
                        //dateParam.OdbcType = OdbcType.DateTime;
                        //dateParam.Value = DateTime.Now.AddHours(-1).AddMinutes(-1).AddSeconds(-15); //Выбираем случаайное значение из списка новых продуктов
                        //cmd.Parameters.Add(dateParam);

                        //OdbcParameter logDateParam = new OdbcParameter();
                        //logDateParam.ParameterName = "@name";
                        //logDateParam.OdbcType = OdbcType.DateTime;
                        //logDateParam.Value = dateParam.Value; //Выбираем случаайное значение из списка новых продуктов
                        //cmd.Parameters.Add(logDateParam);


                        OdbcParameter newnizdParam = new OdbcParameter();
                        newnizdParam.ParameterName = "@nnizd";
                        newnizdParam.OdbcType = OdbcType.Int;
                        newnizdParam.Value = n_izd; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newnizdParam);

                        OdbcParameter newName = new OdbcParameter();
                        newName.ParameterName = "@oname";
                        newName.OdbcType = OdbcType.Text;
                        newName.Value = randProduct[random.Next(randProduct.Length)]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newName);

                        OdbcParameter newOType = new OdbcParameter();
                        newOType.ParameterName = "@oname";
                        newOType.OdbcType = OdbcType.Text;
                        newOType.Value = "Вставка " + tableList[randTable]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newOType);

                        //OdbcParameter newDate = new OdbcParameter();
                        //newDate.ParameterName = "@odate";
                        //newDate.OdbcType = OdbcType.DateTime;
                        //newDate.Value = dateParam.Value; //Выбираем случаайное значение из списка новых продуктов
                        //cmd.Parameters.Add(newDate);

                        //OdbcParameter nizdParam = new OdbcParameter();
                        //nizdParam.ParameterName = "@nizd";
                        //nizdParam.OdbcType = OdbcType.Int;
                        //nizdParam.Value = 1; //Выбираем случаайное значение из списка новых продуктов
                        //cmd.Parameters.Add(nizdParam);

                        break;
                    }
                //DELETE
                case 2:
                    {
                        before = getCurrentTableLog(conn, randTable, "max");
                        if (before == null)
                        {
                            break;
                        }

                        string sqlCmd = "DELETE FROM " + tableList[randTable] + " WHERE n_izd = (SELECT max(n_izd) FROM " + tableList[randTable] + "); INSERT INTO pmib6703.log VALUES(current_timestamp, '" + tableList[randTable] + "', ?,?,?,?,NULL,NULL,NULL,NULL)";
                        cmd = new OdbcCommand(sqlCmd, conn);

                        OdbcParameter oldnizdParam = new OdbcParameter();
                        oldnizdParam.ParameterName = "@onizd";
                        oldnizdParam.OdbcType = OdbcType.Int;
                        oldnizdParam.Value = before[0]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldnizdParam);

                        OdbcParameter oldName = new OdbcParameter();
                        oldName.ParameterName = "@oname";
                        oldName.OdbcType = OdbcType.Text;
                        oldName.Value = before[1]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldName);

                        OdbcParameter oldOType = new OdbcParameter();
                        oldOType.ParameterName = "@oname";
                        oldOType.OdbcType = OdbcType.Text;
                        oldOType.Value = before[2]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldOType);

                        OdbcParameter oldDate = new OdbcParameter();
                        oldDate.ParameterName = "@odate";
                        oldDate.OdbcType = OdbcType.DateTime;
                        oldDate.Value = before[3]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldDate);

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
                Console.WriteLine(opList[i] + cmd.ExecuteNonQuery().ToString());
                tx.Commit();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                tx.Rollback();
            }
            System.Threading.Thread.Sleep(6);
            //after = getCurrentTableLog(conn, randTable);
            conn.Close();

        }

        static void Main(string[] args)
        {
            Program a = new Program();
            if (Console.ReadLine() == "1")
            {
                a.initData();
            }
            else
            {
                while (true)
                {
                    a.imitate();
                    System.Threading.Thread.Sleep(1000);
                }
            }
        }
    }
}
