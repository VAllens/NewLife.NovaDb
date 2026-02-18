using System.Data;
using System.Data.Common;

namespace NewLife.NovaDb.Client;

/// <summary>NovaDb ADO.NET 命令</summary>
public class NovaDbCommand : DbCommand
{
    private String _commandText = String.Empty;
    private readonly NovaDbParameterCollection _parameters = new();

    /// <summary>SQL 命令文本</summary>
    public override String CommandText
    {
        get => _commandText;
        set => _commandText = value ?? String.Empty;
    }

    /// <summary>命令超时（秒）</summary>
    public override Int32 CommandTimeout { get; set; } = 30;

    /// <summary>命令类型</summary>
    public override CommandType CommandType { get; set; } = CommandType.Text;

    /// <summary>是否在设计时可见</summary>
    public override Boolean DesignTimeVisible { get; set; }

    /// <summary>更新行来源</summary>
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <summary>关联连接</summary>
    protected override DbConnection? DbConnection { get; set; }

    /// <summary>参数集合</summary>
    protected override DbParameterCollection DbParameterCollection => _parameters;

    /// <summary>关联事务</summary>
    protected override DbTransaction? DbTransaction { get; set; }

    /// <summary>取消命令</summary>
    public override void Cancel() { }

    /// <summary>执行非查询命令</summary>
    /// <returns>受影响行数</returns>
    public override Int32 ExecuteNonQuery() => 0;

    /// <summary>执行查询并返回第一行第一列</summary>
    /// <returns>标量值</returns>
    public override Object? ExecuteScalar() => null;

    /// <summary>预编译命令</summary>
    public override void Prepare() { }

    /// <summary>创建参数</summary>
    /// <returns>参数实例</returns>
    protected override DbParameter CreateDbParameter() => new NovaDbParameter();

    /// <summary>执行查询并返回数据读取器</summary>
    /// <param name="behavior">命令行为</param>
    /// <returns>数据读取器</returns>
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => new NovaDbDataReader();
}
