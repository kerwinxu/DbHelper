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
            string str_table_name = "test2017061301";
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
            string str_table_name = "test2017061301";
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
    }
}
