/// <summary>  
/// 编 码 人：徐恒晓
/// 联系方式：QQ ： 280287668  
/// </summary>  
using System;
using System.Collections;
using System.Collections.Specialized;
using System.Data;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Data.Common;
using System.Collections.Generic;

namespace Xuhengxiao.DbHelper
{
    /// <summary>
    /// 这个是MySQL的，只是作为简单的助手，另外因为数据量不大，所以这里用每条命令都同时打开连接、运行命令、然后关闭命令的方式。
    /// 这个是2.0版本的，跟上一版本最大的区别是，上个版本的方法大多是静态的，而这个不是。增加了动态创建数据库和表。
    /// </summary>
    /// <summary>  
    /// 数据访问抽象基础类  
    /// </summary>  
    public class DbHelperMySQL2
    {
        #region 连接字符串相关

        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public string connectionString { get; set; }

        /// <summary>
        /// 数据库连接对象，私有的，作为保存用。
        /// </summary>
        private MySqlConnection _conn;
        /// <summary>
        /// 数据库连接对象属性，这个作为全局的，避免多次连接,作为一个属性，好点。
        /// </summary>
        public MySqlConnection Connection
        {
            get
            {
                if (_conn == null)
                {
                    _conn = new MySqlConnection(connectionString);
                    _conn.Open();
                }
                else if (_conn.State == System.Data.ConnectionState.Closed)//关闭
                {
                    _conn.Open();
                }
                else if (_conn.State == System.Data.ConnectionState.Broken)//连接中断，
                {
                    _conn.Close();
                    _conn.Open();
                }
                return _conn;
            }
            set
            {
                _conn = value;
            }
        }

        #endregion

        #region 构造函数

        /// <summary>
        /// 构造函数只是设置连接字符串而已。
        /// </summary>
        /// <param name="str_conn"></param>
        public DbHelperMySQL2(string str_conn)
        {
            connectionString = str_conn;
            //CONNECTION = new MySqlConnection(str_conn);//可以不在这里连接，
        }
        /// <summary>
        /// 详细的设置数据库连接字符串。
        /// </summary>
        /// <param name="server">数据库服务器地址</param>
        /// <param name="database">数据库名称</param>
        /// <param name="userID">登录数据库的用户名</param>
        /// <param name="password">登录数据库的密码</param>
        /// <param name="port">数据库服务器端口,默认3306</param>
        /// <param name="pooling">是否启用连接池（默认启用）</param>
        public DbHelperMySQL2(string server, string database, string userID, string password, uint port=3306, bool pooling = true)
        {
            var builder = new MySqlConnectionStringBuilder()
            {
                Server = server,
                Port = port,
                Database = database,
                IntegratedSecurity = false,
                UserID = userID,
                Password = password,
                Pooling = pooling
            };
            connectionString = builder.ConnectionString;
        }

        #endregion

        #region 关闭数据库
        /// <summary>
        /// 关闭数据库连接。
        /// </summary>
        public void Close()
        {
            if (Connection.State == ConnectionState.Open || Connection.State == ConnectionState.Broken)
            {
                Connection.Close();
            }

        }

        #endregion

        #region  执行不带参数的sql语句，返回受影响的行数，基本上是ExecuteSql

        /// <summary>
        /// 执行sql，并设置等待时间。
        /// </summary>
        /// <param name="SQLString"></param>
        /// <param name="Times">超时时间，默认30秒</param>
        /// <returns></returns>
        public int ExecuteNonQuery(string SQLString, int Times=30)
        {
            using (MySqlCommand cmd = new MySqlCommand(SQLString, Connection))
            {
                try
                {

                    cmd.CommandTimeout = Times;
                    int rows = cmd.ExecuteNonQuery();
                    return rows;
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {
                    Connection.Close();
                    throw e;
                }
            }

        }

        /// <summary>  
        /// 执行多条SQL语句，实现数据库事务。  
        /// </summary>  
        /// <param name="SQLStringList">多条SQL语句</param>       
        public int ExecuteSqlTran(List<String> SQLStringList)
        {


            using (MySqlCommand cmd = new MySqlCommand())
            {

                cmd.Connection = Connection;
                MySqlTransaction tx = Connection.BeginTransaction();
                cmd.Transaction = tx;
                try
                {
                    int count = 0;
                    for (int n = 0; n < SQLStringList.Count; n++)
                    {
                        string strsql = SQLStringList[n];
                        if (strsql.Trim().Length > 1)
                        {
                            cmd.CommandText = strsql;
                            count += cmd.ExecuteNonQuery();
                        }
                    }
                    tx.Commit();
                    return count;
                }
                catch
                {
                    tx.Rollback();
                    return 0;
                }


            }
        }

        /// <summary>  
        /// 执行带一个存储过程参数的的SQL语句，斌。  
        /// </summary>  
        /// <param name="SQLString">SQL语句</param>  
        /// <param name="content">参数内容,比如一个字段是格式复杂的文章，有特殊符号，可以通过这个方式添加</param>  
        /// <returns>影响的记录数</returns>  
        public int ExecuteNonQuery(string SQLString, string content)
        {


            MySqlCommand cmd = new MySqlCommand(SQLString, Connection);
            MySql.Data.MySqlClient.MySqlParameter myParameter = new MySql.Data.MySqlClient.MySqlParameter("@content", SqlDbType.NText);
            myParameter.Value = content;
            cmd.Parameters.Add(myParameter);
            try
            {

                int rows = cmd.ExecuteNonQuery();
                return rows;
            }
            catch (MySql.Data.MySqlClient.MySqlException e)
            {
                Connection.Close();
                throw e;

            }
            finally
            {
                cmd.Dispose();

            }

        }

        /// <summary>  
        /// 向数据库里插入图像格式的字段(和上面情况类似的另一种实例)  
        /// </summary>  
        /// <param name="strSQL">SQL语句</param>  
        /// <param name="fs">图像字节,数据库的字段类型为image的情况</param>  
        /// <returns>影响的记录数</returns>  
        public int ExecuteSqlInsertImg(string strSQL, byte[] fs)
        {


            MySqlCommand cmd = new MySqlCommand(strSQL, Connection);
            MySql.Data.MySqlClient.MySqlParameter myParameter = new MySql.Data.MySqlClient.MySqlParameter("@fs", SqlDbType.Image);
            myParameter.Value = fs;
            cmd.Parameters.Add(myParameter);
            try
            {
                int rows = cmd.ExecuteNonQuery();
                return rows;
            }
            catch (MySql.Data.MySqlClient.MySqlException e)
            {
                Connection.Close();
                throw e;
            }
            finally
            {
                cmd.Dispose();

            }

        }

        /// <summary>  
        /// 执行带一个存储过程参数的的SQL语句。  
        /// </summary>  
        /// <param name="SQLString">SQL语句</param>  
        /// <param name="content">参数内容,比如一个字段是格式复杂的文章，有特殊符号，可以通过这个方式添加</param>  
        /// <returns>影响的记录数</returns>  
        public object ExecuteSqlGet(string SQLString, string content)
        {

            MySqlCommand cmd = new MySqlCommand(SQLString, Connection);
            MySql.Data.MySqlClient.MySqlParameter myParameter = new MySql.Data.MySqlClient.MySqlParameter("@content", SqlDbType.NText);
            myParameter.Value = content;
            cmd.Parameters.Add(myParameter);
            try
            {

                object obj = cmd.ExecuteScalar();
                if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                {
                    return null;
                }
                else
                {
                    return obj;
                }
            }
            catch (MySql.Data.MySqlClient.MySqlException e)
            {
                Connection.Close();

                throw e;
            }
            finally
            {
                cmd.Dispose();

            }

        }

        #endregion

        #region  执行sql只是返回第一行第一列的数据。ExecuteScalar

        /// <summary>
        /// 执行一条计算查询结果语句，只是返回第一列第一行的数据，返回查询结果（object）
        /// </summary>
        /// <param name="SQLString"></param>
        /// <param name="Times">超时时间，默认30秒</param>
        /// <returns></returns>
        public object ExecuteScalar(string SQLString, int Times=30)
        {



            using (MySqlCommand cmd = new MySqlCommand(SQLString, Connection))
            {
                try
                {
                    
                    cmd.CommandTimeout = Times;
                    object obj = cmd.ExecuteScalar();
                    if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {
                    Connection.Close();
                    throw e;
                }
            }

        }

        #endregion

        #region 执行sql，返回返回 MySqlDataReader，DataTable，DataSet

        /// <summary>  
        /// 执行查询语句，返回MySqlDataReader ( 注意：调用该方法后，一定要对MySqlDataReader进行Close )  
        /// </summary>  
        /// <param name="strSQL">查询语句</param>  
        /// <returns>MySqlDataReader</returns>  
        public MySqlDataReader ExecuteReader(string strSQL)
        {
            using (MySqlCommand cmd = new MySqlCommand(strSQL, Connection))
            {
                try
                {

                    MySqlDataReader myReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                    return myReader;
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {
                    Connection.Close();

                    throw e;
                }
            }



        }

        /// <summary>
        /// 执行sql语句，有超时时间，返回DataTable
        /// </summary>
        /// <param name="SQLString"></param>
        /// <param name="Times">超时时间，默认30秒</param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string SQLString, int Times=30)
        {
            DataTable ds = new DataTable();
            try
            {

                MySqlDataAdapter command = new MySqlDataAdapter(SQLString, Connection);
                command.SelectCommand.CommandTimeout = Times;
                command.Fill(ds);
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Connection.Close();
                throw new Exception(ex.Message);
            }
            return ds;

        }

        /// <summary>
        /// 执行sql语句，有超时时间，返回DataSet
        /// </summary>
        /// <param name="SQLString"></param>
        /// <param name="Times">>超时时间，默认30秒</param>
        /// <returns></returns>
        public DataSet ExecuteDataSet(string SQLString, int Times=30)
        {

            DataSet ds = new DataSet();
            try
            {

                MySqlDataAdapter command = new MySqlDataAdapter(SQLString, Connection);
                command.SelectCommand.CommandTimeout = Times;
                command.Fill(ds);
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Connection.Close();
                throw new Exception(ex.Message);
            }
            return ds;
        }
        /// <summary>
        /// 执行sql，返回DataSet，但是是可以修改这个
        /// </summary>
        /// <param name="SQLString"></param>
        /// <param name="adapter"></param>
        /// <param name="Times"></param>
        /// <returns></returns>
        public DataSet ExecuteDataSet(string SQLString, out MySqlDataAdapter adapter, int Times = 30)
        {
            DataSet ds = new DataSet();
            try
            {

                adapter = new MySqlDataAdapter(SQLString, Connection);
                adapter.SelectCommand.CommandTimeout = Times;
                adapter.Fill(ds);
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Connection.Close();
                throw new Exception(ex.Message);
            }
            return ds;
        }


        #endregion

        #region 执行带参数的SQL语句  

        /// <summary>  
        /// 执行SQL语句，返回影响的记录数  
        /// </summary>  
        /// <param name="SQLString">SQL语句</param>  
        /// <returns>影响的记录数</returns>  
        public int ExecuteNonQuery(string SQLString, params MySqlParameter[] cmdParms)
        {


            using (MySqlCommand cmd = new MySqlCommand())
            {

                try
                {
                    PrepareCommand(cmd, Connection, null, SQLString, cmdParms);
                    int rows = cmd.ExecuteNonQuery();

                    return rows;
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {
                    Connection.Close();
                    throw e;
                }
                finally
                {
                    cmd.Parameters.Clear();//参数清零
                }
            }

        }

        /// <summary>  
        /// 执行多条SQL语句，实现数据库事务。  
        /// </summary>  
        /// <param name="SQLStringList">SQL语句的哈希表（key为sql语句，value是该语句的MySqlParameter[]）</param>  
        public void ExecuteSqlTran(Hashtable SQLStringList)
        {

            using (MySqlTransaction trans = Connection.BeginTransaction())
            {
                MySqlCommand cmd = new MySqlCommand();

                try
                {
                    //循环  
                    foreach (DictionaryEntry myDE in SQLStringList)
                    {
                        string cmdText = myDE.Key.ToString();
                        MySqlParameter[] cmdParms = (MySqlParameter[])myDE.Value;
                        PrepareCommand(cmd, Connection, trans, cmdText, cmdParms);
                        int val = cmd.ExecuteNonQuery();

                    }
                    trans.Commit();
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {

                    trans.Rollback();
                    Connection.Close();
                    throw e;
                }
                finally
                {
                    cmd.Parameters.Clear();//参数清零
                }
            }

        }

        /// <summary>  
        /// 执行多条SQL语句，实现数据库事务。  
        /// </summary>  
        /// <param name="SQLStringList">SQL语句的哈希表（key为sql语句，value是该语句的MySqlParameter[]）</param>  
        public int ExecuteSqlTran(System.Collections.Generic.List<CommandInfo> cmdList)
        {



            using (MySqlTransaction trans = Connection.BeginTransaction())
            {
                MySqlCommand cmd = new MySqlCommand();

                try
                {
                    int count = 0;
                    //循环  
                    foreach (CommandInfo myDE in cmdList)
                    {
                        string cmdText = myDE.CommandText;
                        MySqlParameter[] cmdParms = (MySqlParameter[])myDE.Parameters;
                        PrepareCommand(cmd, Connection, trans, cmdText, cmdParms);

                        if (myDE.EffentNextType == EffentNextType.WhenHaveContine || myDE.EffentNextType == EffentNextType.WhenNoHaveContine)
                        {
                            if (myDE.CommandText.ToLower().IndexOf("count(") == -1)
                            {
                                trans.Rollback();
                                return 0;
                            }

                            object obj = cmd.ExecuteScalar();
                            bool isHave = false;
                            if (obj == null && obj == DBNull.Value)
                            {
                                isHave = false;
                            }
                            isHave = Convert.ToInt32(obj) > 0;

                            if (myDE.EffentNextType == EffentNextType.WhenHaveContine && !isHave)
                            {
                                trans.Rollback();
                                return 0;
                            }
                            if (myDE.EffentNextType == EffentNextType.WhenNoHaveContine && isHave)
                            {
                                trans.Rollback();
                                return 0;
                            }
                            continue;
                        }
                        int val = cmd.ExecuteNonQuery();
                        count += val;
                        if (myDE.EffentNextType == EffentNextType.ExcuteEffectRows && val == 0)
                        {
                            trans.Rollback();
                            return 0;
                        }

                    }
                    trans.Commit();
                    return count;
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {
                    Connection.Close();
                    trans.Rollback();
                    throw e;
                }
                finally
                {
                    cmd.Parameters.Clear();//首先参数清零
                }

            }
        }

        /// <summary>  
        /// 执行多条SQL语句，实现数据库事务。  
        /// </summary>  
        /// <param name="SQLStringList">SQL语句的哈希表（key为sql语句，value是该语句的MySqlParameter[]）</param>  
        public void ExecuteSqlTranWithIndentity(System.Collections.Generic.List<CommandInfo> SQLStringList)
        {


         
            using (MySqlTransaction trans = Connection.BeginTransaction())
            {
                MySqlCommand cmd = new MySqlCommand();

                try
                {
                    int indentity = 0;
                    //循环  
                    foreach (CommandInfo myDE in SQLStringList)
                    {
                        string cmdText = myDE.CommandText;
                        MySqlParameter[] cmdParms = (MySqlParameter[])myDE.Parameters;
                        foreach (MySqlParameter q in cmdParms)
                        {
                            if (q.Direction == ParameterDirection.InputOutput)
                            {
                                q.Value = indentity;
                            }
                        }
                        PrepareCommand(cmd, Connection, trans, cmdText, cmdParms);
                        int val = cmd.ExecuteNonQuery();
                        foreach (MySqlParameter q in cmdParms)
                        {
                            if (q.Direction == ParameterDirection.Output)
                            {
                                indentity = Convert.ToInt32(q.Value);
                            }
                        }

                    }
                    trans.Commit();
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {
                    trans.Rollback();
                    Connection.Close();
                    throw e;
                }
                finally
                {
                    cmd.Parameters.Clear();//参数清零，为了下一次节约利用。
                }
            }

        }

        /// <summary>  
        /// 执行多条SQL语句，实现数据库事务。  
        /// </summary>  
        /// <param name="SQLStringList">SQL语句的哈希表（key为sql语句，value是该语句的MySqlParameter[]）</param>  
        public void ExecuteSqlTranWithIndentity(Hashtable SQLStringList)
        {

           
            using (MySqlTransaction trans = Connection.BeginTransaction())
            {
                MySqlCommand cmd = new MySqlCommand();

                try
                {
                    int indentity = 0;
                    //循环  
                    foreach (DictionaryEntry myDE in SQLStringList)
                    {
                        string cmdText = myDE.Key.ToString();
                        MySqlParameter[] cmdParms = (MySqlParameter[])myDE.Value;
                        foreach (MySqlParameter q in cmdParms)
                        {
                            if (q.Direction == ParameterDirection.InputOutput)
                            {
                                q.Value = indentity;
                            }
                        }
                        PrepareCommand(cmd, Connection, trans, cmdText, cmdParms);
                        int val = cmd.ExecuteNonQuery();
                        foreach (MySqlParameter q in cmdParms)
                        {
                            if (q.Direction == ParameterDirection.Output)
                            {
                                indentity = Convert.ToInt32(q.Value);
                            }
                        }

                    }
                    trans.Commit();
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {
                    Connection.Close();
                    trans.Rollback();
                    throw e;
                }
                finally
                {
                    cmd.Parameters.Clear();//参数清零，为了下一次节约利用。
                }
            }

        }

        /// <summary>  
        /// 执行一条计算查询结果语句，返回查询结果（object）。  
        /// </summary>  
        /// <param name="SQLString">计算查询结果语句</param>  
        /// <returns>查询结果（object）</returns>  
        public object ExecuteScalar(string SQLString, params MySqlParameter[] cmdParms)
        {



            using (MySqlCommand cmd = new MySqlCommand())
            {

                try
                {
                    PrepareCommand(cmd, Connection, null, SQLString, cmdParms);
                    object obj = cmd.ExecuteScalar();

                    if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
                catch (MySql.Data.MySqlClient.MySqlException e)
                {
                    Connection.Close();
                    throw e;
                }
                finally
                {
                    cmd.Parameters.Clear();//参数清零，为了下一次节约利用。
                }
            }

        }

        /// <summary>  
        /// 执行查询语句，返回MySqlDataReader ( 注意：调用该方法后，一定要对MySqlDataReader进行Close )  
        /// </summary>  
        /// <param name="strSQL">查询语句</param>  
        /// <returns>MySqlDataReader</returns>  
        public MySqlDataReader ExecuteReader(string SQLString, params MySqlParameter[] cmdParms)
        {


            MySqlCommand cmd = new MySqlCommand();

            try
            {
                PrepareCommand(cmd, Connection, null, SQLString, cmdParms);
                MySqlDataReader myReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                return myReader;
            }
            catch (MySql.Data.MySqlClient.MySqlException e)
            {
                Connection.Close();
                throw e;
            }
            finally
            {

                cmd.Parameters.Clear();
                cmd.Dispose();
            }

        }

        /// <summary>  
        /// 执行查询语句，返回DataTable 
        /// </summary>  
        /// <param name="SQLString">查询语句</param>  
        /// <returns>DataSet</returns>  
        public DataTable ExecuteDataTable(string SQLString, params MySqlParameter[] cmdParms)
        {


            MySqlCommand cmd = new MySqlCommand();
            PrepareCommand(cmd, Connection, null, SQLString, cmdParms);
            using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
            {
                DataTable ds = new DataTable();
                try
                {
                    da.Fill(ds);

                }
                catch (MySql.Data.MySqlClient.MySqlException ex)
                {
                    Connection.Close();
                    throw new Exception(ex.Message);
                }
                finally
                {
                    cmd.Parameters.Clear();//参数清零，为了下一次节约利用。
                }
                return ds;
            }

        }

        /// <summary>  
        /// 执行查询语句，返回DataSet  
        /// </summary>  
        /// <param name="SQLString">查询语句</param>  
        /// <returns>DataSet</returns>  
        public DataSet ExecuteDataSet(string SQLString, params MySqlParameter[] cmdParms)
        {

            MySqlCommand cmd = new MySqlCommand();

            PrepareCommand(cmd, Connection, null, SQLString, cmdParms);
            using (MySqlDataAdapter da = new MySqlDataAdapter(cmd))
            {
                DataSet ds = new DataSet();
                try
                {
                    da.Fill(ds);

                }
                catch (MySql.Data.MySqlClient.MySqlException ex)
                {
                    Connection.Close();
                    throw new Exception(ex.Message);
                }
                finally
                {
                    cmd.Parameters.Clear();//参数清零，为了下一次节约利用。
                }
                return ds;
            }

        }

        /// <summary>
        /// 筹备MySqlCommand,基本上就是将参数数组迭代给MySqlCommand
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="conn"></param>
        /// <param name="trans"></param>
        /// <param name="strSQL"></param>
        /// <param name="cmdParms"></param>
        private void PrepareCommand(MySqlCommand cmd, MySqlConnection conn, MySqlTransaction trans, string strSQL, MySqlParameter[] cmdParms)
        {
            //如果不是打开状态，就打开连接。
            if (conn.State != ConnectionState.Open)
                conn.Open();
            //设置MySqlCommand的连接
            cmd.Connection = conn;
            //设置SQL语句
            cmd.CommandText = strSQL;
            //如果支持事务的，就设置
            if (trans != null)
                cmd.Transaction = trans;
            cmd.CommandType = CommandType.Text;//cmdType;  
            //如果参数数组不为空
            if (cmdParms != null)
            {
                //就迭代它
                foreach (MySqlParameter parameter in cmdParms)
                {

                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) &&
                        (parameter.Value == null))
                    {
                        parameter.Value = DBNull.Value;
                    }
                    //添加这个参数
                    cmd.Parameters.Add(parameter);
                }
            }
        }

        #endregion

        #region 动态创建数据库和表相关

        #region 判断数据库是否存在  
        /// <summary>  
        /// 判断数据库是否存在  
        /// </summary>  
        /// <param name="db">数据库的名称</param>  
        /// <returns>true:表示数据库已经存在；false，表示数据库不存在</returns>  
        public Boolean IsDBExist(string db)
        {
            //SELECT * FROM information_schema.SCHEMATA where SCHEMA_NAME='aa';
            string createDbStr = "SELECT * FROM information_schema.SCHEMATA where SCHEMA_NAME='" + db + "';";

            DataTable dt = ExecuteDataTable(createDbStr);
            if (dt.Rows.Count == 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        #endregion

        #region 判断数据库中，指定表是否存在  
        /// <summary>  
        /// 判断数据库表是否存在  
        /// </summary>  
        /// <param name="db">数据库</param>  
        /// <param name="tb">数据表名</param>  
        /// <returns>true:表示数据表已经存在；false，表示数据表不存在</returns>  
        public Boolean IsTableExist(string db, string tb)
        {
            //在指定的数据库中  查找 该表是否存在 
            // use business_one;
            //SHOW TABLES LIKE 'outlook_contact';

            string createDbStr = "use " + db + "; SHOW TABLES LIKE '" + tb + "';";

            //用查询的方式查是否有。
            DataTable dt = ExecuteDataTable(createDbStr);
            if (dt.Rows.Count == 0)
            {
                return false;
            }
            else
            {
                return true;
            }

        }
        #endregion

        #region 创建数据库  
        /// <summary>  
        /// 创建数据库  
        /// </summary>  
        /// <param name="db">数据库名称</param>  
        public void CreateDataBase(string db)
        {

            Boolean flag = IsDBExist(db);

            //如果数据库存在，则抛出  
            if (flag == true)
            {
                throw new Exception("数据库已经存在！");
            }
            else
            {
                //数据库不存在，创建数据库  
                string createDbStr = "Create database " + db;
                ExecuteNonQuery(createDbStr);

            }

        }
        #endregion

        #region 创建数据库表  
        /// <summary>  
        ///  在指定的数据库中，创建数据表  
        /// </summary>  
        /// <param name="db">指定的数据库</param>  
        /// <param name="dt">要创建的数据表</param>  
        /// <param name="dic">数据表中的字段及其数据类型</param>  
        public void CreateDataTable(string db, string dt, Dictionary<string, string> dic)
        {

            //判断数据库是否存在  
            if (IsDBExist(db) == false)
            {
                throw new Exception("数据库不存在！");
            }

            //如果数据库表存在，则抛出错误  
            if (IsTableExist(db, dt) == true)
            {
                throw new Exception("数据库表已经存在！");
            }
            else//数据表不存在，创建数据表  
            {
                //拼接字符串，（该串为创建内容）  
                string content = "ID int(20) primary key not null auto_increment ";
                //取出dic中的内容，进行拼接  
                List<string> test = new List<string>(dic.Keys);
                for (int i = 0; i < dic.Count; i++)
                {
                    content = content + " , " + test[i] + " " + dic[test[i]];
                }

                //其后判断数据表是否存在，然后创建数据表  
                string createTableStr = "use " + db + "; create table " + dt + " (" + content + ");";

                ExecuteNonQuery(createTableStr);
            }
        }

        #endregion

        #endregion

        #region 公用方法  包括得到最大值，是否存在之类的

        /// <summary>  
        /// 得到最大值  
        /// </summary>  
        /// <param name="FieldName"></param>  
        /// <param name="TableName"></param>  
        /// <returns></returns>  
        public int GetMaxID(string FieldName, string TableName)
        {
            string strsql = "select max(" + FieldName + ")+1 from " + TableName;
            object obj = ExecuteScalar(strsql);
            //如果值为空，就返回0，否则就转换数据啦。
            if (obj == null)
            {
                return 0;
            }
            else
            {
                return int.Parse(obj.ToString());
            }
        }
        /// <summary>  
        /// 是否存在  
        /// </summary>  
        /// <param name="strSql"></param>  
        /// <returns></returns>  
        public bool Exists(string strSql)
        {
            object obj = ExecuteScalar(strSql);
            int cmdresult;
            //判断返回值。
            if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
            {
                cmdresult = 0;
            }
            else
            {
                cmdresult = int.Parse(obj.ToString());
            }

            if (cmdresult == 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        /// <summary>  
        /// 是否存在（基于MySqlParameter）  
        /// </summary>  
        /// <param name="strSql"></param>  
        /// <param name="cmdParms"></param>  
        /// <returns></returns>  
        public bool Exists(string strSql, params MySqlParameter[] cmdParms)
        {
            object obj = ExecuteScalar(strSql, cmdParms);
            int cmdresult;
            if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
            {
                cmdresult = 0;
            }
            else
            {
                cmdresult = int.Parse(obj.ToString());
            }
            if (cmdresult == 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        #endregion

    }
}
