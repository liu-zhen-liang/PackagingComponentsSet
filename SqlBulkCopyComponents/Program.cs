using System;
using System.Data.SqlClient;
using System.Diagnostics;

namespace SqlBulkCopyComponents
{
    class Program
    {
        static void Main(string[] args)
        {
            #region 生成测试数据
            
            var persons = new Person[100000];
            var random = new Random();
            for (int i = 0; i < persons.Length; i++)
            {
                persons[i] = new Person
                {
                    Id = i + 1,
                    Name = "张三" + i,
                    Age = random.Next(1, 128),
                    Sex = (Gender)random.Next(2),
                    CreateTime = random.Next(2) == 0 ? null : (DateTime?)DateTime.Now.AddSeconds(i),
                };
            }

            #endregion

            //清除测试表数据
            using (var conn = new SqlConnection("Server=.;Database=DemoDataBase;User ID=sa;Password=8888;"))
            {
                conn.Open();
                using (var com = new SqlCommand("TRUNCATE TABLE dbo.Person", conn))
                {
                    com.ExecuteNonQuery();
                }
            }

            //创建数据库连接
            using (var conn = new SqlConnection("Server=.;Database=DemoDataBase;User ID=sa;Password=8888;"))
            {
                conn.Open();
                var sw = Stopwatch.StartNew();
                //批量插入数据
                var qty = conn.BulkCopy(persons);
                sw.Stop();
                Console.WriteLine(qty);
                Console.WriteLine(sw.Elapsed.TotalMilliseconds + "ms");
            }
        }
    }

    /* 创表语句
    CREATE TABLE [dbo].[Person](
	    [Id] [BIGINT] NOT NULL,
	    [Name] [VARCHAR](64) NOT NULL,
	    [Age] [INT] NOT NULL,
	    [CreateTime] [DATETIME] NULL,
	    [Sex] [INT] NOT NULL,
    PRIMARY KEY CLUSTERED 
    (
	    [Id] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
    ) ON [PRIMARY]
     */

    public class Person
    {
        public long Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public DateTime? CreateTime { get; set; }
        public Gender Sex { get; set; }
    }

    public enum Gender
    {
        Man = 0,
        Woman = 1
    }
}
