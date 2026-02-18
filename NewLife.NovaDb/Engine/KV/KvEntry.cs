namespace NewLife.NovaDb.Engine.KV;

/// <summary>KV 存储条目</summary>
public class KvEntry
{
    /// <summary>键</summary>
    public String Key { get; set; } = String.Empty;

    /// <summary>值</summary>
    public Byte[]? Value { get; set; }

    /// <summary>过期时间（UTC），null 表示永不过期</summary>
    public DateTime? ExpiresAt { get; set; }

    /// <summary>创建时间</summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    /// <summary>最后修改时间</summary>
    public DateTime ModifiedAt { get; set; } = DateTime.UtcNow;

    /// <summary>检查是否已过期</summary>
    public Boolean IsExpired() => ExpiresAt.HasValue && DateTime.UtcNow >= ExpiresAt.Value;
}
