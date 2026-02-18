using System.Text;
using NewLife.NovaDb.Core;

namespace NewLife.NovaDb.Engine.KV;

/// <summary>KV 键值存储引擎，支持按行 TTL</summary>
public class KvStore
{
    private readonly Dictionary<String, KvEntry> _data = new(StringComparer.Ordinal);
    private readonly Object _lock = new();
    private readonly DbOptions? _options;

    /// <summary>非过期键的数量</summary>
    public Int32 Count
    {
        get
        {
            lock (_lock)
            {
                var count = 0;
                foreach (var entry in _data.Values)
                {
                    if (!entry.IsExpired())
                        count++;
                }
                return count;
            }
        }
    }

    /// <summary>创建 KvStore 实例</summary>
    /// <param name="options">数据库选项，可为 null</param>
    public KvStore(DbOptions? options = null)
    {
        _options = options;
    }

    /// <summary>设置键值对</summary>
    /// <param name="key">键</param>
    /// <param name="value">值</param>
    /// <param name="ttl">过期时间，null 表示永不过期</param>
    public void Set(String key, Byte[]? value, TimeSpan? ttl = null)
    {
        if (String.IsNullOrEmpty(key)) throw new ArgumentException("键不能为空", nameof(key));

        lock (_lock)
        {
            var now = DateTime.UtcNow;
            if (_data.TryGetValue(key, out var existing))
            {
                existing.Value = value;
                existing.ExpiresAt = ttl.HasValue ? now.Add(ttl.Value) : null;
                existing.ModifiedAt = now;
            }
            else
            {
                _data[key] = new KvEntry
                {
                    Key = key,
                    Value = value,
                    ExpiresAt = ttl.HasValue ? now.Add(ttl.Value) : null,
                    CreatedAt = now,
                    ModifiedAt = now,
                };
            }
        }
    }

    /// <summary>获取键对应的值，过期则惰性删除并返回 null</summary>
    /// <param name="key">键</param>
    /// <returns>值，不存在或已过期返回 null</returns>
    public Byte[]? Get(String key)
    {
        if (String.IsNullOrEmpty(key)) throw new ArgumentException("键不能为空", nameof(key));

        lock (_lock)
        {
            if (!_data.TryGetValue(key, out var entry))
                return null;

            // 惰性删除
            if (entry.IsExpired())
            {
                _data.Remove(key);
                return null;
            }

            return entry.Value;
        }
    }

    /// <summary>删除键</summary>
    /// <param name="key">键</param>
    /// <returns>键存在并被删除返回 true，否则返回 false</returns>
    public Boolean Delete(String key)
    {
        if (String.IsNullOrEmpty(key)) throw new ArgumentException("键不能为空", nameof(key));

        lock (_lock)
        {
            return _data.Remove(key);
        }
    }

    /// <summary>检查键是否存在且未过期</summary>
    /// <param name="key">键</param>
    /// <returns>存在且未过期返回 true</returns>
    public Boolean Exists(String key)
    {
        if (String.IsNullOrEmpty(key)) throw new ArgumentException("键不能为空", nameof(key));

        lock (_lock)
        {
            if (!_data.TryGetValue(key, out var entry))
                return false;

            // 惰性删除
            if (entry.IsExpired())
            {
                _data.Remove(key);
                return false;
            }

            return true;
        }
    }

    /// <summary>设置字符串键值对（UTF-8 编码）</summary>
    /// <param name="key">键</param>
    /// <param name="value">字符串值</param>
    /// <param name="ttl">过期时间，null 表示永不过期</param>
    public void SetString(String key, String value, TimeSpan? ttl = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));

        Set(key, Encoding.UTF8.GetBytes(value), ttl);
    }

    /// <summary>获取字符串值（UTF-8 解码）</summary>
    /// <param name="key">键</param>
    /// <returns>字符串值，不存在或已过期返回 null</returns>
    public String? GetString(String key)
    {
        var bytes = Get(key);
        if (bytes == null) return null;

        return Encoding.UTF8.GetString(bytes);
    }

    /// <summary>获取键的过期时间</summary>
    /// <param name="key">键</param>
    /// <returns>过期时间，键不存在返回 null</returns>
    public DateTime? GetExpiration(String key)
    {
        if (String.IsNullOrEmpty(key)) throw new ArgumentException("键不能为空", nameof(key));

        lock (_lock)
        {
            if (!_data.TryGetValue(key, out var entry))
                return null;

            return entry.ExpiresAt;
        }
    }

    /// <summary>更新键的过期时间</summary>
    /// <param name="key">键</param>
    /// <param name="ttl">新的 TTL</param>
    /// <returns>键存在并更新成功返回 true</returns>
    public Boolean SetExpiration(String key, TimeSpan ttl)
    {
        if (String.IsNullOrEmpty(key)) throw new ArgumentException("键不能为空", nameof(key));

        lock (_lock)
        {
            if (!_data.TryGetValue(key, out var entry))
                return false;

            entry.ExpiresAt = DateTime.UtcNow.Add(ttl);
            entry.ModifiedAt = DateTime.UtcNow;
            return true;
        }
    }

    /// <summary>清理所有已过期的条目</summary>
    /// <returns>删除的条目数量</returns>
    public Int32 CleanupExpired()
    {
        var toRemove = new List<String>();

        lock (_lock)
        {
            foreach (var kvp in _data)
            {
                if (kvp.Value.IsExpired())
                    toRemove.Add(kvp.Key);
            }

            foreach (var key in toRemove)
            {
                _data.Remove(key);
            }
        }

        return toRemove.Count;
    }

    /// <summary>获取所有未过期的键</summary>
    /// <returns>未过期键列表</returns>
    public List<String> GetAllKeys()
    {
        var keys = new List<String>();

        lock (_lock)
        {
            foreach (var kvp in _data)
            {
                if (!kvp.Value.IsExpired())
                    keys.Add(kvp.Key);
            }
        }

        return keys;
    }
}
