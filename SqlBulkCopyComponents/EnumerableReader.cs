using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkCopyComponents
{
    /// <summary>
    /// 迭代器数据读取器
    /// </summary>
    /// <typeparam name="TModel">模型类型</typeparam>
    public class EnumerableReader<TModel> : IDataReader
    {
        /// <summary>
        /// 实例化迭代器读取对象
        /// </summary>
        /// <param name="source">模型源</param>
        public EnumerableReader(IEnumerable<TModel> source)
        {
            _source = source ?? throw new ArgumentNullException(nameof(source));
            _enumerable = source.GetEnumerator();
        }

        private readonly IEnumerable<TModel> _source;
        private readonly IEnumerator<TModel> _enumerable;
        private object[] _currentDataRow = Array.Empty<object>();
        private int _depth;
        private bool _release;

        public void Dispose()
        {
            _release = true;
            _enumerable.Dispose();
        }

        public int GetValues(object[] values)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            var length = Math.Min(_currentDataRow.Length, values.Length);
            Array.Copy(_currentDataRow, values, length);
            return length;
        }

        public int GetOrdinal(string name)
        {
            for (int i = 0; i < ModelToDataTable<TModel>.Columns.Count; i++)
            {
                if (ModelToDataTable<TModel>.Columns[i].ColumnName == name) return i;
            }

            return -1;
        }

        public long GetBytes(int ordinal, long dataIndex, byte[] buffer, int bufferIndex, int length)
        {
            if (dataIndex < 0) throw new Exception($"起始下标不能小于0！");
            if (bufferIndex < 0) throw new Exception("目标缓冲区起始下标不能小于0！");
            if (length < 0) throw new Exception("读取长度不能小于0！");
            var numArray = (byte[])GetValue(ordinal);
            if (buffer == null) return numArray.Length;
            if (buffer.Length <= bufferIndex) throw new Exception("目标缓冲区起始下标不能大于目标缓冲区范围！");
            var freeLength = Math.Min(numArray.Length - bufferIndex, length);
            if (freeLength <= 0) return 0;
            Array.Copy(numArray, dataIndex, buffer, bufferIndex, length);
            return freeLength;
        }

        public long GetChars(int ordinal, long dataIndex, char[] buffer, int bufferIndex, int length)
        {
            if (dataIndex < 0) throw new Exception($"起始下标不能小于0！");
            if (bufferIndex < 0) throw new Exception("目标缓冲区起始下标不能小于0！");
            if (length < 0) throw new Exception("读取长度不能小于0！");
            var numArray = (char[])GetValue(ordinal);
            if (buffer == null) return numArray.Length;
            if (buffer.Length <= bufferIndex) throw new Exception("目标缓冲区起始下标不能大于目标缓冲区范围！");
            var freeLength = Math.Min(numArray.Length - bufferIndex, length);
            if (freeLength <= 0) return 0;
            Array.Copy(numArray, dataIndex, buffer, bufferIndex, length);
            return freeLength;
        }

        public bool IsDBNull(int i)
        {
            var value = GetValue(i);
            return value == null || value is DBNull;
        }
        public bool NextResult()
        {
            //移动到下一个元素
            if (!_enumerable.MoveNext()) return false;
            //行层+1
            Interlocked.Increment(ref _depth);
            //得到数据行
            _currentDataRow = ModelToDataTable<TModel>.ToRowData.Invoke(_enumerable.Current);
            return true;
        }

        public byte GetByte(int i) => (byte)GetValue(i);
        public string GetName(int i) => ModelToDataTable<TModel>.Columns[i].ColumnName;
        public string GetDataTypeName(int i) => ModelToDataTable<TModel>.Columns[i].DataType.Name;
        public Type GetFieldType(int i) => ModelToDataTable<TModel>.Columns[i].DataType;
        public object GetValue(int i) => _currentDataRow[i];
        public bool GetBoolean(int i) => (bool)GetValue(i);
        public char GetChar(int i) => (char)GetValue(i);
        public Guid GetGuid(int i) => (Guid)GetValue(i);
        public short GetInt16(int i) => (short)GetValue(i);
        public int GetInt32(int i) => (int)GetValue(i);
        public long GetInt64(int i) => (long)GetValue(i);
        public float GetFloat(int i) => (float)GetValue(i);
        public double GetDouble(int i) => (double)GetValue(i);
        public string GetString(int i) => (string)GetValue(i);
        public decimal GetDecimal(int i) => (decimal)GetValue(i);
        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
        public IDataReader GetData(int i) => throw new NotSupportedException();
        public int FieldCount => ModelToDataTable<TModel>.Columns.Count;
        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(GetOrdinal(name));
        public void Close() => Dispose();
        public DataTable GetSchemaTable() => ModelToDataTable<TModel>.ToDataTable(_source);
        public bool Read() => NextResult();
        public int Depth => _depth;
        public bool IsClosed => _release;
        public int RecordsAffected => 0;
    }
}
