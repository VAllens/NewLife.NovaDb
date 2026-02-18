using NewLife.Remoting;

namespace NewLife.NovaDb.Server;

/// <summary>NovaDb RPC 服务控制器，提供数据库操作接口</summary>
/// <remarks>
/// 控制器方法通过 Remoting RPC 暴露为远程接口。
/// 路由格式：Nova/{方法名}，如 Nova/Ping、Nova/Execute。
/// </remarks>
internal class NovaController : IApi
{
    /// <summary>会话</summary>
    public IApiSession Session { get; set; } = null!;

    /// <summary>心跳</summary>
    /// <returns>服务器时间</returns>
    public String Ping() => DateTime.UtcNow.ToString("o");

    /// <summary>执行 SQL（非查询）</summary>
    /// <param name="sql">SQL 语句</param>
    /// <returns>受影响行数</returns>
    public Int32 Execute(String sql)
    {
        // TODO: 接入 SQL 引擎
        return 0;
    }

    /// <summary>查询</summary>
    /// <param name="sql">SQL 语句</param>
    /// <returns>查询结果（JSON）</returns>
    public Object? Query(String sql)
    {
        // TODO: 接入 SQL 引擎
        return null;
    }

    /// <summary>开始事务</summary>
    /// <returns>事务 ID</returns>
    public String BeginTransaction()
    {
        // TODO: 接入事务管理器
        return Guid.NewGuid().ToString("N");
    }

    /// <summary>提交事务</summary>
    /// <param name="txId">事务 ID</param>
    /// <returns>是否成功</returns>
    public Boolean CommitTransaction(String txId)
    {
        // TODO: 接入事务管理器
        return true;
    }

    /// <summary>回滚事务</summary>
    /// <param name="txId">事务 ID</param>
    /// <returns>是否成功</returns>
    public Boolean RollbackTransaction(String txId)
    {
        // TODO: 接入事务管理器
        return true;
    }
}
