using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;

namespace Xuhengxiao.DbHelper
{
    /// <summary>
    /// 经过测试，这个是连接sql server的。
    /// </summary>
    public class DbHelperSQLServer
    {
        private SqlConnection conn;
        private SqlCommand cmd;
        private SqlDataReader reader;
        private SqlDataAdapter adapter;
        private string connectionString = @"server=.;database=student;uid=sa;pwd=scce";

        public string ConnectionString
        {
            get { return this.connectionString; }
            set { this.connectionString = value; }
        }

        /// <summary>
        /// 获取一个未打开连接的SqlConnection对象
        /// </summary>
        /// <returns>SqlConnection对象</returns>
        public SqlConnection GetConnection()
        {
            if (conn != null)
                return this.conn;
            return this.conn = new SqlConnection(connectionString);
        }

        /// <summary>
        /// 使用连接字符串获取未打开连接SqlConnection对象
        /// </summary>
        /// <param name="_connStr">连接字符串</param>
        /// <returns>SqlConnection对象</returns>
        public SqlConnection GetConnection(string _connStr)
        {
            if (this.conn != null)
                this.conn.ConnectionString = _connStr;
            else
                this.conn = new SqlConnection(_connStr);
            return this.conn;
        }

        /// <summary>
        /// 使用指定的Sql语句创建SqlCommand对象
        /// </summary>
        /// <param name="sqlStr">Sql语句</param>
        /// <returns>SqlCommand对象</returns>
        private SqlCommand GetCommand(string sqlStr)
        {
            if (this.conn == null)
                this.conn = GetConnection();
            if (this.cmd == null)
                this.cmd = this.GetCommand(sqlStr, CommandType.Text, null);
            else
            {
                this.cmd.CommandType = CommandType.Text;
                this.cmd.Parameters.Clear();
            }
            this.cmd.CommandText = sqlStr;
            return this.cmd;
        }

        /// <summary>
        /// 使用指定的Sql语句,CommandType,SqlParameter数组创建SqlCommand对象
        /// </summary>
        /// <param name="sqlStr">Sql语句</param>
        /// <param name="type">命令类型</param>
        /// <param name="paras">SqlParameter数组</param>
        /// <returns>SqlCommand对象</returns>
        public SqlCommand GetCommand(string sqlStr, CommandType type, SqlParameter[] paras)
        {
            if (conn == null)
                this.conn = this.GetConnection();
            if (cmd == null)
                this.cmd = conn.CreateCommand();
            this.cmd.CommandType = type;
            this.cmd.CommandText = sqlStr;
            this.cmd.Parameters.Clear();
            if (paras != null)
                this.cmd.Parameters.AddRange(paras);
            return this.cmd;
        }

        /// <summary>
        /// 执行Sql语句返回受影响的行数
        /// </summary>
        /// <param name="sqlStr">Sql语句</param>
        /// <returns>受影响的行数,失败则返回-1</returns>
        public int ExecuteNoQuery(string sqlStr)
        {
            int line = -1;
            CheckArgs(sqlStr);
            try { OpenConn(); line = this.ExecuteNonQuery(sqlStr, CommandType.Text, null); }
            catch (SqlException e) { throw e; }
            return line;
        }

        /// <summary>
        /// 使用指定的Sql语句,CommandType,SqlParameter数组执行Sql语句,返回受影响的行数
        /// </summary>
        /// <param name="sqlStr">Sql语句</param>
        /// <param name="type">命令类型</param>
        /// <param name="paras">SqlParameter数组</param>
        /// <returns>受影响的行数</returns>
        public int ExecuteNonQuery(string sqlStr, CommandType type, SqlParameter[] paras)
        {
            int line = -1;
            CheckArgs(sqlStr);
            if (this.cmd == null)
                GetCommand(sqlStr, type, paras);
            this.cmd.Parameters.Clear();
            this.cmd.CommandText = sqlStr;
            this.cmd.CommandType = type;
            if (paras != null)
                this.cmd.Parameters.AddRange(paras);
            try { OpenConn(); line = this.cmd.ExecuteNonQuery(); }
            catch (SqlException e) { throw e; }
            return line;
        }

        /// <summary>
        /// 使用指定Sql语句获取dataTable
        /// </summary>
        /// <param name="sqlStr">Sql语句</param>
        /// <returns>DataTable对象</returns>
        public DataTable GetDataTable(string sqlStr)
        {
            CheckArgs(sqlStr);
            if (this.conn == null)
                this.conn = GetConnection();
            this.adapter = new SqlDataAdapter(sqlStr, this.conn);
            DataTable table = new DataTable();
            try { adapter.Fill(table); }
            catch (SqlException e) { throw e; }
            return table;
        }

        /// <summary>
        /// 使用指定的Sql语句获取SqlDataReader
        /// </summary>
        /// <param name="sqlStr">sql语句</param>
        /// <returns>SqlDataReader对象</returns>
        public SqlDataReader GetSqlDataReader(string sqlStr)
        {
            CheckArgs(sqlStr);
            if (cmd == null)
                GetCommand(sqlStr);
            if (reader != null)
                reader.Dispose();
            try { OpenConn(); this.reader = this.cmd.ExecuteReader(); }
            catch (SqlException e) { throw e; }
            return this.reader;
        }

        /// <summary>
        /// 使用事务执行多条Sql语句
        /// </summary>
        /// <param name="sqlCommands">sql语句数组</param>
        /// <returns>全部成功则返回true否则返回false</returns>
        public bool ExecuteSqls(string[] sqlCommands)
        {
            if (sqlCommands == null)
                throw new ArgumentNullException();
            if (this.cmd == null)
                GetCommand(null);
            SqlTransaction tran = null;
            try
            {
                OpenConn();
                tran = this.conn.BeginTransaction();
                this.cmd.Transaction = tran;
                foreach (string sql in sqlCommands)
                {
                    if (ExecuteNoQuery(sql) == 0)
                    { tran.Rollback(); return false; }
                }
            }
            catch (SqlException e)
            {
                if (tran != null)
                    tran.Rollback();
                throw e;
            }
            tran.Commit();
            return true;
        }

        private void OpenConn()
        {
            try
            {
                if (this.conn.State == ConnectionState.Closed)
                    conn.Open();
            }
            catch (SqlException e) { throw e; }
        }

        private void CheckArgs(string sqlStr)
        {
            if (sqlStr == null)
                throw new ArgumentNullException();
            if (sqlStr.Length == 0)
                throw new ArgumentOutOfRangeException();
        }

    }
}
