using System;
using System.Collections;
using System.Data.Odbc;

namespace BD_KR_MY_INS
{
    class Program
    {
        private static string[] tableList = new string[] { "pmib6703.p1", "pmib6703.p2", "pmib6703.p3", "pmib6703.c" };

        private string[] randProduct = new string[] { "realme 5 Pro", "Blade 20", "iPhone 8", "OnePlus 7", "Moto G8", "vivo Y19", "Xperia L3" };

        private static string[] vals = new string[] {"'Pocophone F1'",
        "'Pixel 4'",
        "'iPhone XR'",
        "'Redmi Note 8'",
        "'Galaxy A10'",
        "'Honor 9X'",
        "'Mi 9 Lite'",
        "'Honor 10'",
        "'Xperia 1'",
        "'realme 5'",
        "'Mi Note 10'",
        "'Mi 9T'",
        "'iPhone 11'",
        "'iPhone 11 Pro'",
        "'realme XT'"};

        //функция подключения к нашей БД
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

        //Очистка заданной БД
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

        //Сброс заданного счетчика
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


        //программа инициализации
        private void initData()
        {
            OdbcConnection conn = connectToDB();
            truncTable(conn, "pmib6703.journal");
            truncTable(conn, "pmib6703.rtime");
            resetSeq(conn, "pmib6703.rtime" + "_n_seq");

            //очищаем все таблицы и сбрасываем у них счетчики уникальных идентификаторов
            for (int i = 0; i < tableList.Length; i++)
            {
                truncTable(conn, tableList[i]);
                resetSeq(conn, tableList[i] + "_n_seq");

            }

            //По каждому названию
            for (int j = 0; j < vals.Length; j++)
            {
                OdbcTransaction tx = null;

                try
                {
                    tx = conn.BeginTransaction();
                    for (int i = 0; i < tableList.Length; i++)
                    {
                        //Получаем счетчик
                        int n = getCounter(i);
                        OdbcCommand tmp = new OdbcCommand("INSERT INTO " + tableList[i] + " VALUES (?," + vals[j] + ",'Начальная вставка', current_timestamp); INSERT INTO pmib6703.journal VALUES (current_timestamp, '" + tableList[i] + "', NULL, NULL, NULL, NULL, ?, " + vals[j] + ",'Начальная вставка', current_timestamp);", conn);

                        OdbcParameter nParam = new OdbcParameter();
                        nParam.ParameterName = "@n";
                        nParam.OdbcType = OdbcType.Int;
                        nParam.Value = n; //Выбираем случаайное значение из списка новых продуктов
                        tmp.Parameters.Add(nParam);

                        OdbcParameter nnParam = new OdbcParameter();
                        nnParam.ParameterName = "@nn";
                        nnParam.OdbcType = OdbcType.Int;
                        nnParam.Value = n; //Выбираем случаайное значение из списка новых продуктов
                        tmp.Parameters.Add(nnParam);


                        tmp.Transaction = tx;
                        Console.WriteLine("Вставлена строка " + n.ToString() + " " + vals[j] + " " + tmp.ExecuteNonQuery().ToString());
                    }
                    //OdbcCommand journalInto = new OdbcCommand("INSERT INTO pmib6703.journal");
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

        //Получаем строку, которую будем модифицировать (важно для лога)
        private ArrayList getCurrentTableLog(OdbcConnection conn, int randTable, string min)
        {
            OdbcCommand selectCmd = new OdbcCommand("SELECT * FROM " + tableList[randTable] + " WHERE n = (SELECT " + min + "(n) FROM " + tableList[randTable] + ")", conn);

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

        //получаем уникальный идентификатор для заданной таблицы
        private int getCounter(int randTable)
        {
            OdbcConnection conn = connectToDB();
            OdbcCommand selectCmd = new OdbcCommand("SELECT nextval('" + tableList[randTable] + "_n_seq')", conn);
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

        //ИРС
        private void imitate()
        {
            string[] opList = { "update ", "insert ", "delete " };

            Random random = new Random();
            ArrayList before = new ArrayList();
            ArrayList after = new ArrayList();
            OdbcConnection conn = connectToDB();
            int i = random.Next(3);
            //Debug
            int randTable = random.Next(tableList.Length - 1);

            OdbcCommand cmd = null;
            switch (i)
            {
                //update
                case 0:
                    {
                        before = getCurrentTableLog(conn, randTable, "min");
                        string sqlCmd = "UPDATE " + tableList[randTable] + " SET product = ?, date = current_timestamp, operation = 'Обновление " + tableList[randTable] + "' WHERE n = (SELECT min(n) FROM " + tableList[randTable] + "); INSERT INTO pmib6703.journal VALUES(current_timestamp, '" + tableList[randTable] + "', ?,?,?,?,?,?,?,current_timestamp)";
                        cmd = new OdbcCommand(sqlCmd, conn);

                        OdbcParameter productParam = new OdbcParameter();
                        productParam.ParameterName = "@product";
                        productParam.OdbcType = OdbcType.Text;
                        productParam.Value = randProduct[random.Next(randProduct.Length)]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(productParam);

                        OdbcParameter oldnParam = new OdbcParameter();
                        oldnParam.ParameterName = "@on";
                        oldnParam.OdbcType = OdbcType.Int;
                        oldnParam.Value = before[0]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldnParam);

                        OdbcParameter oldName = new OdbcParameter();
                        oldName.ParameterName = "@oproduct";
                        oldName.OdbcType = OdbcType.Text;
                        oldName.Value = before[1]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldName);

                        OdbcParameter oldOperation = new OdbcParameter();
                        oldOperation.ParameterName = "@oproduct";
                        oldOperation.OdbcType = OdbcType.Text;
                        oldOperation.Value = before[2]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldOperation);

                        OdbcParameter oldDate = new OdbcParameter();
                        oldDate.ParameterName = "@odate";
                        oldDate.OdbcType = OdbcType.DateTime;
                        oldDate.Value = before[3]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldDate);


                        OdbcParameter newnParam = new OdbcParameter();
                        newnParam.ParameterName = "@nn";
                        newnParam.OdbcType = OdbcType.Int;
                        newnParam.Value = before[0]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newnParam);

                        OdbcParameter newName = new OdbcParameter();
                        newName.ParameterName = "@oproduct";
                        newName.OdbcType = OdbcType.Text;
                        newName.Value = productParam.Value; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newName);

                        OdbcParameter newOperation = new OdbcParameter();
                        newOperation.ParameterName = "@oproduct";
                        newOperation.OdbcType = OdbcType.Text;
                        newOperation.Value = "Обновление " + tableList[randTable]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newOperation);

                        break;
                    }
                //insert
                case 1:
                    {
                        int n = getCounter(randTable);

                        string sqlCmd = "INSERT INTO " + tableList[randTable] + " VALUES (?, ?, 'Вставка " + tableList[randTable] + "', current_timestamp); INSERT INTO pmib6703.journal VALUES(current_timestamp, '" + tableList[randTable] + "', NULL,NULL,NULL,NULL,?,?,?,current_timestamp)";
                        cmd = new OdbcCommand(sqlCmd, conn);

                        OdbcParameter nParam = new OdbcParameter();
                        nParam.ParameterName = "@n";
                        nParam.OdbcType = OdbcType.Int;
                        nParam.Value = n; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(nParam);

                        OdbcParameter productParam = new OdbcParameter();
                        productParam.ParameterName = "@product";
                        productParam.OdbcType = OdbcType.Text;
                        productParam.Value = randProduct[random.Next(randProduct.Length)]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(productParam);

                        OdbcParameter newnParam = new OdbcParameter();
                        newnParam.ParameterName = "@nn";
                        newnParam.OdbcType = OdbcType.Int;
                        newnParam.Value = n; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newnParam);

                        OdbcParameter newName = new OdbcParameter();
                        newName.ParameterName = "@oproduct";
                        newName.OdbcType = OdbcType.Text;
                        newName.Value = productParam.Value; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newName);

                        OdbcParameter newOperation = new OdbcParameter();
                        newOperation.ParameterName = "@oproduct";
                        newOperation.OdbcType = OdbcType.Text;
                        newOperation.Value = "Вставка " + tableList[randTable]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(newOperation);

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

                        string sqlCmd = "DELETE FROM " + tableList[randTable] + " WHERE n = (SELECT max(n) FROM " + tableList[randTable] + "); INSERT INTO pmib6703.journal VALUES(current_timestamp, '" + tableList[randTable] + "', ?,?,?,?,NULL,NULL,NULL,NULL)";
                        cmd = new OdbcCommand(sqlCmd, conn);

                        OdbcParameter oldnParam = new OdbcParameter();
                        oldnParam.ParameterName = "@on";
                        oldnParam.OdbcType = OdbcType.Int;
                        oldnParam.Value = before[0]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldnParam);

                        OdbcParameter oldName = new OdbcParameter();
                        oldName.ParameterName = "@oproduct";
                        oldName.OdbcType = OdbcType.Text;
                        oldName.Value = before[1]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldName);

                        OdbcParameter oldOperation = new OdbcParameter();
                        oldOperation.ParameterName = "@oproduct";
                        oldOperation.OdbcType = OdbcType.Text;
                        oldOperation.Value = before[2]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldOperation);

                        OdbcParameter oldDate = new OdbcParameter();
                        oldDate.ParameterName = "@odate";
                        oldDate.OdbcType = OdbcType.DateTime;
                        oldDate.Value = before[3]; //Выбираем случаайное значение из списка новых продуктов
                        cmd.Parameters.Add(oldDate);

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
            //Время между разными разными продуктами
            System.Threading.Thread.Sleep(100);

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
                    System.Threading.Thread.Sleep(1000); //Время повтора запуска ИРД
                }
            }
        }
    }
}
