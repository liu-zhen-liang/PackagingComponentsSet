using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkCopyComponents
{
    public static class SqlConnectionExtension
    {
        /// <summary>
        /// 批量复制
        /// </summary>
        /// <typeparam name="TModel">插入的模型对象</typeparam>
        /// <param name="source">需要批量插入的数据源</param>
        /// <param name="connection">数据库连接对象</param>
        /// <param name="tableName">插入表名称【为NULL默认为实体名称】</param>
        /// <param name="bulkCopyTimeout">插入超时时间</param>
        /// <param name="batchSize">写入数据库一批数量【如果为0代表全部一次性插入】最合适数量【这取决于您的环境，尤其是行数和网络延迟。就个人而言，我将从将BatchSize属性设置为1000行开始，然后看看其性能如何。如果可行，那么我将使行数加倍（例如增加到2000、4000等），直到性能下降或超时。否则，如果超时发生在1000，那么我将行数减少一半（例如500），直到它起作用为止。】</param>
        /// <param name="options">批量复制参数</param>
        /// <param name="externalTransaction">执行的事务对象</param>
        /// <returns>插入数量</returns>
        public static int BulkCopy<TModel>(this SqlConnection connection,
            IEnumerable<TModel> source,
            string tableName = null,
            int bulkCopyTimeout = 30,
            int batchSize = 0,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            SqlTransaction externalTransaction = null)
        {
            //创建读取器
            using (var reader = new EnumerableReader<TModel>(source))
            {
                //创建批量插入对象
                using (var copy = new SqlBulkCopy(connection, options, externalTransaction))
                {
                    //插入的表
                    copy.DestinationTableName = tableName ?? typeof(TModel).Name;
                    //写入数据库一批数量
                    copy.BatchSize = batchSize;
                    //超时时间
                    copy.BulkCopyTimeout = bulkCopyTimeout;
                    //创建字段映射【如果没有此字段映射会导致数据填错位置，如果类型不对还会导致报错】【因为：没有此字段映射默认是按照列序号对应插入的】
                    foreach (var column in ModelToDataTable<TModel>.Columns)
                    {
                        //创建字段映射
                        copy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }
                    //将数据批量写入数据库
                    copy.WriteToServer(reader);
                    //返回插入数据数量
                    return reader.Depth;
                }
            }
        }

        /// <summary>
        /// 批量复制-异步
        /// </summary>
        /// <typeparam name="TModel">插入的模型对象</typeparam>
        /// <param name="source">需要批量插入的数据源</param>
        /// <param name="connection">数据库连接对象</param>
        /// <param name="tableName">插入表名称【为NULL默认为实体名称】</param>
        /// <param name="bulkCopyTimeout">插入超时时间</param>
        /// <param name="batchSize">写入数据库一批数量【如果为0代表全部一次性插入】最合适数量【这取决于您的环境，尤其是行数和网络延迟。就个人而言，我将从将BatchSize属性设置为1000行开始，然后看看其性能如何。如果可行，那么我将使行数加倍（例如增加到2000、4000等），直到性能下降或超时。否则，如果超时发生在1000，那么我将行数减少一半（例如500），直到它起作用为止。】</param>
        /// <param name="options">批量复制参数</param>
        /// <param name="externalTransaction">执行的事务对象</param>
        /// <returns>插入数量</returns>
        public static async Task<int> BulkCopyAsync<TModel>(this SqlConnection connection,
            IEnumerable<TModel> source,
            string tableName = null,
            int bulkCopyTimeout = 30,
            int batchSize = 0,
            SqlBulkCopyOptions options = SqlBulkCopyOptions.Default,
            SqlTransaction externalTransaction = null)
        {
            //创建读取器
            using (var reader = new EnumerableReader<TModel>(source))
            {
                //创建批量插入对象
                using (var copy = new SqlBulkCopy(connection, options, externalTransaction))
                {
                    //插入的表
                    copy.DestinationTableName = tableName ?? typeof(TModel).Name;
                    //写入数据库一批数量
                    copy.BatchSize = batchSize;
                    //超时时间
                    copy.BulkCopyTimeout = bulkCopyTimeout;
                    //创建字段映射【如果没有此字段映射会导致数据填错位置，如果类型不对还会导致报错】【因为：没有此字段映射默认是按照列序号对应插入的】
                    foreach (var column in ModelToDataTable<TModel>.Columns)
                    {
                        //创建字段映射
                        copy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }
                    //将数据批量写入数据库
                    await copy.WriteToServerAsync(reader);
                    //返回插入数据数量
                    return reader.Depth;
                }
            }
        }
    }
}
