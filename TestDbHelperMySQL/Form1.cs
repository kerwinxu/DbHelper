using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using Xuhengxiao.DbHelper;
using System.Diagnostics;
using MySql.Data.MySqlClient;

namespace TestDbHelperMySQL
{
    public partial class Form1 : Form
    {
        //很多表都拿这个来做处理。
        string str_table_name = "test2017061301";

        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            dataGridView1.DataSource = null;
            DbHelperMySQL.connectionString = "Server=localhost;Database=gnucash; Uid=cashuser;Pwd=wodegnucash;";
            try
            {
                DataTable dt= DbHelperMySQL.ExecuteDataTable("use gnucash;select * from accounts;");
                dataGridView1.DataSource = dt;


            }
            catch (Exception error)
            {
                MessageBox.Show(error.Message);
                //throw;
            }
            sw.Stop();
            MessageBox.Show(sw.ElapsedMilliseconds.ToString());

        }

        private void button2_Click(object sender, EventArgs e)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            dataGridView1.DataSource = null;
            DbHelperMySQL.connectionString = "Server=localhost;Database=gnucash; Uid=cashuser;Pwd=wodegnucash;";
            try
            {
                DataSet dt = DbHelperMySQL.ExecuteDataSet("use gnucash;select * from accounts;");
                dataGridView1.DataSource = dt.Tables[0];


            }
            catch (Exception error)
            {
                MessageBox.Show(error.Message);
                //throw;
            }
            sw.Stop();
            MessageBox.Show(sw.ElapsedMilliseconds.ToString());
        }

        private void button3_Click(object sender, EventArgs e)
        {
            //DbHelperMySQL2 db = new DbHelperMySQL2("Server=localhost;Database=business_one; Uid=business;Pwd=nicaibudaola111;");
            DbHelperMySQL2 db = new DbHelperMySQL2("localhost","business_one","business", "nicaibudaola111");
            //先删除这个表

            db.ExecuteNonQuery("DROP TABLE IF EXISTS " + str_table_name + ";");

            Dictionary<string, string> dict = new Dictionary<string, string>();
            dict.Add("姓名", "VARCHAR(200)");
            db.CreateDataTable("business_one", str_table_name, dict);
            dataGridView1.DataSource = db.ExecuteDataTable("use business_one;select * from "+ str_table_name);
        }

        private void button4_Click(object sender, EventArgs e)
        {
            DbHelperMySQL2 db = new DbHelperMySQL2("Server=localhost;Database=business_one; Uid=business;Pwd=nicaibudaola111;");
            //先删除这个表

            db.ExecuteNonQuery("DROP TABLE IF EXISTS " + str_table_name+";");
            //然后再新建表
            Dictionary<string, string> dict = new Dictionary<string, string>();
            dict.Add("姓名", "VARCHAR(200)");
            db.CreateDataTable("business_one", str_table_name, dict);
            //然后添加数据
            string sql_insert = "insert into " + str_table_name + "(姓名) values(@china_name);";
            MySqlParameter par1 = new MySqlParameter("@china_name", MySqlDbType.VarChar, 200);
            par1.Value = "徐恒晓";
            MySqlParameter[] par = { par1 };
            //执行插入
            db.ExecuteNonQuery(sql_insert, par);
            //查看数据
            dataGridView1.DataSource = db.ExecuteDataTable("use business_one;select * from "+ str_table_name+";");
        }

        private MySqlDataAdapter MySqlDataAdapter_1;
        private void button5_Click(object sender, EventArgs e)
        {
            //这个方法看看能够更新到数据库中的方法。
            DbHelperMySQL2 db = new DbHelperMySQL2("Server=localhost;Database=business_one; Uid=business;Pwd=nicaibudaola111;");
            //查看数据,并保存MySqlDataAdapter
            dataGridView1.DataSource = db.ExecuteDataSet("use business_one;select * from " + str_table_name + ";",out MySqlDataAdapter_1).Tables[0];
            
            //添加InsertCommand
            string str_insert_sql = "insert into "+str_table_name+ "(ID,姓名) values(@ID,@chinesename);";
            MySqlDataAdapter_1.InsertCommand = new MySqlCommand(str_insert_sql, db.Connection);
            MySqlDataAdapter_1.InsertCommand.Parameters.Add("@ID", MySqlDbType.Int64,20,"ID");
            MySqlDataAdapter_1.InsertCommand.Parameters.Add("@chinesename", MySqlDbType.VarChar, 200, "姓名");

            //添加UpdateCommand
            string str_update_sql = "update " + str_table_name + " set 姓名=@chinesename where ID=@ID;";
            MySqlDataAdapter_1.UpdateCommand = new MySqlCommand(str_update_sql, db.Connection);
            MySqlDataAdapter_1.UpdateCommand.Parameters.Add("@ID", MySqlDbType.Int64, 20, "ID");
            MySqlDataAdapter_1.UpdateCommand.Parameters.Add("@chinesename", MySqlDbType.VarChar, 200, "姓名");

            //添加DeleteCommand
            string str_delete_sql = "delete from "+str_table_name+ " where ID=@ID;";
            MySqlDataAdapter_1.DeleteCommand = new MySqlCommand(str_delete_sql, db.Connection);
            MySqlDataAdapter_1.DeleteCommand.Parameters.Add("@ID", MySqlDbType.Int64, 20, "ID");

            bindingSource1.DataSource = dataGridView1.DataSource;

        }

        private void button6_Click(object sender, EventArgs e)
        {
            //失败了，得定义InsertCommand等等。
            MySqlDataAdapter_1.Update((DataTable)dataGridView1.DataSource);

        }

        private void button7_Click(object sender, EventArgs e)
        {
            //这个方法看看能够更新到数据库中的方法。
            DbHelperMySQL2 db = new DbHelperMySQL2("Server=localhost;Database=business_one; Uid=business;Pwd=nicaibudaola111;");
            //查看数据,并保存MySqlDataAdapter
            dataGridView1.DataSource = db.ExecuteDataTable("use business_one;select * from " + str_table_name + ";", out MySqlDataAdapter_1);
            MySqlCommandBuilder cb = new MySqlCommandBuilder(MySqlDataAdapter_1);//增加了这个，就会自动生成UpdateCommand,InsertCommand,DeleteCommand，前提是表有主键。
            //但这个方式，修改主键的情况下，不会更改到数据库中，估计原因是 "update 表 set 主键=新值 where 主键=新值",如果要修改主键，得另外做一个update，最好是单独的一个执行。
            bindingSource1.DataSource = dataGridView1.DataSource;

        }
    }
}
