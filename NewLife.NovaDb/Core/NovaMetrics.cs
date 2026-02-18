namespace NewLife.NovaDb.Core;

/// <summary>NovaDb 运行时指标</summary>
public class NovaMetrics
{
    /// <summary>表数量</summary>
    public Int32 TableCount { get; set; }

    /// <summary>总行数（所有表）</summary>
    public Int64 TotalRows { get; set; }

    /// <summary>SQL 执行次数</summary>
    public Int64 ExecuteCount { get; set; }

    /// <summary>查询次数</summary>
    public Int64 QueryCount { get; set; }

    /// <summary>插入次数</summary>
    public Int64 InsertCount { get; set; }

    /// <summary>更新次数</summary>
    public Int64 UpdateCount { get; set; }

    /// <summary>删除次数</summary>
    public Int64 DeleteCount { get; set; }

    /// <summary>DDL 次数</summary>
    public Int64 DdlCount { get; set; }

    /// <summary>事务提交次数</summary>
    public Int64 CommitCount { get; set; }

    /// <summary>事务回滚次数</summary>
    public Int64 RollbackCount { get; set; }

    /// <summary>启动时间</summary>
    public DateTime StartTime { get; set; }

    /// <summary>运行时长</summary>
    public TimeSpan Uptime => DateTime.Now - StartTime;
}
