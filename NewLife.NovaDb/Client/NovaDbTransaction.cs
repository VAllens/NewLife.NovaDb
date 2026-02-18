using System.Data;
using System.Data.Common;

namespace NewLife.NovaDb.Client;

/// <summary>NovaDb ADO.NET 事务</summary>
public class NovaDbTransaction : DbTransaction
{
    private readonly NovaDbConnection _connection;
    private Boolean _completed;

    /// <summary>创建事务实例</summary>
    /// <param name="connection">关联的连接</param>
    public NovaDbTransaction(NovaDbConnection connection)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <summary>隔离级别</summary>
    public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

    /// <summary>关联的连接</summary>
    protected override DbConnection DbConnection => _connection;

    /// <summary>是否已完成</summary>
    public Boolean IsCompleted => _completed;

    /// <summary>提交事务</summary>
    public override void Commit() => _completed = true;

    /// <summary>回滚事务</summary>
    public override void Rollback() => _completed = true;
}
