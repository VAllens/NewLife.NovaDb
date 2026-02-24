# NewLife.NovaDb KV 存储架构

> 对应模块：K01（KV 表与 TTL）、K02（KV API）
> 关联模块：W01-W03（WAL 刷盘策略）

---

## 1. 概述

KV 存储引擎采用 **Bitcask 模型**，每个 KV 表对应一个独立的 `.kvd` 数据文件。内存仅保存键到文件偏移的索引（`KvEntry`），值保留在磁盘按需读取，实现低内存占用与高性能的平衡。

### 核心特点

| 特性 | 说明 |
|------|------|
| **存储模型** | Bitcask（内存仅索引约 20B/key，值留磁盘按需读取） |
| **并发** | ConcurrentDictionary 无锁查询，文件 IO 通过 `_writeLock` 串行化 |
| **持久化** | 顺序追加写入 + CRC32 校验，按 WAL 模式控制刷盘 |
| **启动恢复** | 大文件（>64KB）通过 MemoryMappedFile 快速扫描，小文件通过 FileStream |
| **TTL** | 惰性删除 + 主动清理，支持 UTC 时间精度 |
| **自动压缩** | 写入记录数与存活键数比值超阈值时自动 Compact |
| **ICache 集成** | 通过 NovaCache 适配器实现 NewLife.Core 的 ICache 接口 |

---

## 2. 多 KV 表

一个 NovaDb 数据库之下，支持多个 KV 表，每个 KV 表对应数据库目录下的一个 `.kvd` 文件：

```
/data/mydb/
  ├── default.kvd      # 默认 KV 表（服务器模式）
  ├── session.kvd      # 会话缓存表
  ├── lock.kvd         # 分布式锁表
  └── counter.kvd      # 计数器表
```

### 表管理

```csharp
// 嵌入式模式：通过 EmbeddedDatabase 获取 KV 表
var db = new EmbeddedDatabase(options);
var sessionStore = db.GetKvStore("session");   // → session.kvd
var lockStore = db.GetKvStore("lock");         // → lock.kvd

// 服务器模式：NovaServer 自动创建 default.kvd
```

### 固定 Schema

| 列名 | 类型 | 说明 |
|------|------|------|
| Key | String | 键（UTF-8 编码，2B 长度前缀，最大 65535 字节） |
| Value | Byte[]? | 值（可为 null，4B 长度前缀） |
| ExpiresAt | DateTime? | UTC 过期时间（null = 永不过期） |

---

## 3. 文件格式

### 3.1 文件头（32 字节）

使用统一的 `FileHeader` 结构：

| 偏移 | 长度 | 字段 | 说明 |
|------|------|------|------|
| 0 | 4B | Magic | 统一文件头魔数 |
| 4 | 1B | Version | 当前版本 = 3 |
| 5 | 1B | FileType | `FileType.KvData` |
| 6 | 26B | Reserved | PageSize/CreateTime/CRC32 等 |

### 3.2 记录格式

每条记录格式：`[TotalLength: 4B] [RecordType: 1B] [Data: variable] [CRC32: 4B]`

CRC32 校验范围覆盖 `RecordType + Data`，不含 TotalLength 和 CRC 本身。

#### 记录类型

| 类型 | 值 | Data 格式 |
|------|-----|----------|
| Set | 1 | `[KeyLen: 2B] [Key: UTF-8] [ExpiresAt: 8B] [ValueLen: 4B] [Value?]` |
| Delete | 2 | `[KeyLen: 2B] [Key: UTF-8]` |
| Clear | 3 | （空） |

**v3 格式变更**：
- ExpiresAt 始终写入（8B Ticks），永不过期时写 `DateTime.MaxValue.Ticks`
- 移除 Flags 字节，简化记录格式
- Value 为 null 时 ValueLen = 0，不写 Value 数据

---

## 4. API 设计

### 4.1 KvStore 类

`KvStore` 实现 `IDisposable`，一个实例对应一个 `.kvd` 文件。采用 Bitcask 模型，内存仅存索引（`KvEntry`），值留在磁盘按需读取：

```csharp
public partial class KvStore : IDisposable
{
    // 构造
    public KvStore(DbOptions? options, String filePath);       // filePath 必填

    // 基本操作
    public void Set(String key, Byte[]? value, TimeSpan? ttl = null);
    public IOwnerPacket? Get(String key);                      // 池化数据包，用完需 Dispose
    public Boolean TryGet(String key, out IOwnerPacket? value);
    public Boolean Delete(String key);
    public Boolean Exists(String key);
    public void Clear();

    // 字符串便捷方法
    public void SetString(String key, String value, TimeSpan? ttl = null);
    public String? GetString(String key);

    // 高级操作
    public Boolean Add(String key, Byte[] value, TimeSpan ttl);       // 分布式锁
    public Boolean AddString(String key, String value, TimeSpan ttl);
    public IOwnerPacket? Replace(String key, Byte[]? value, TimeSpan? ttl = null);
    public Int64 Inc(String key, Int64 delta = 1, TimeSpan? ttl = null);
    public Double IncDouble(String key, Double delta, TimeSpan? ttl = null);

    // 批量操作
    public IDictionary<String, IOwnerPacket?> GetAll(IEnumerable<String> keys);
    public void SetAll(IDictionary<String, Byte[]?> values, TimeSpan? ttl = null);
    public Int32 Delete(IEnumerable<String> keys);

    // TTL 管理
    public DateTime? GetExpiration(String key);
    public Boolean SetExpiration(String key, TimeSpan ttl);    // Zero = 永不过期
    public TimeSpan GetTtl(String key);
    public Int32 CleanupExpired();

    // 搜索与枚举
    public IEnumerable<String> GetAllKeys();
    public IEnumerable<String> Search(String pattern, Int32 offset = 0, Int32 count = -1);
    public Int32 DeleteByPattern(String pattern);

    // 持久化维护
    public void Compact();          // 压缩数据文件（临时文件替换策略）
    public Int32 Count { get; }     // 非过期键数量
    public String FilePath { get; }
}
```

### 4.2 关键实现

#### 内存索引项（KvEntry）

Bitcask 模型的核心：内存仅存文件偏移信息，每个索引项约 20 字节：

```csharp
public struct KvEntry
{
    public Int64 ValueOffset;      // 值在文件中的起始偏移（-1 = null 值）
    public Int32 ValueLength;      // 值的字节长度
    public DateTime ExpiresAt;     // 过期时间（DateTime.MaxValue = 永不过期）

    public readonly Boolean IsExpired() => ExpiresAt < DateTime.MaxValue && DateTime.UtcNow >= ExpiresAt;
}
```

百万级键仅占约 20MB 内存，不受值大小影响。

#### Set — UPSERT 语义

```csharp
public void Set(String key, Byte[]? value, TimeSpan? ttl = null)
{
    // 未指定 TTL 时使用默认 TTL；无默认 TTL 则永不过期
    var expiresAt = ttl != null ? DateTime.UtcNow.Add(ttl.Value)
        : _defaultTtl != null ? DateTime.UtcNow.Add(_defaultTtl.Value)
        : DateTime.MaxValue;

    lock (_writeLock)
    {
        var valueOffset = WriteSetRecordNoLock(key, value ?? [], expiresAt);
        _data[key] = new KvEntry
        {
            ValueOffset = valueOffset,
            ValueLength = value?.Length ?? 0,
            ExpiresAt = expiresAt,
        };
        TryAutoCompactNoLock();
    }
}
```

#### Get — 惰性 TTL 检查 + 磁盘按需读取

```csharp
public IOwnerPacket? Get(String key)
{
    lock (_writeLock)
    {
        if (!_data.TryGetValue(key, out var index)) return null;
        if (index.IsExpired()) { _data.TryRemove(key, out _); return null; }
        return ReadValueFromDiskNoLock(index);  // 根据偏移从磁盘读取值
    }
}
```

#### Add — 分布式锁（原子 CAS）

```csharp
public Boolean Add(String key, Byte[] value, TimeSpan ttl)
{
    lock (_writeLock)
    {
        if (_data.TryGetValue(key, out var index) && !index.IsExpired())
            return false;       // 键已存在且未过期

        var expiresAt = DateTime.UtcNow.Add(ttl);
        var valueOffset = WriteSetRecordNoLock(key, value, expiresAt);
        _data[key] = new KvEntry { ValueOffset = valueOffset, ValueLength = value.Length, ExpiresAt = expiresAt };
        TryAutoCompactNoLock();
        return true;
    }
}
```

#### Inc / IncDouble — 原子计数器

值以**二进制格式**存储（Int64 / Double，8 字节），通过 `_writeLock` 保证原子性：

```csharp
public Int64 Inc(String key, Int64 delta = 1, TimeSpan? ttl = null)
{
    lock (_writeLock)
    {
        Int64 newValue;
        if (_data.TryGetValue(key, out var index) && !index.IsExpired())
        {
            using var currentPk = ReadValueFromDiskNoLock(index);
            var current = (currentPk != null && currentPk.Length >= 8) ? new SpanReader(currentPk).ReadInt64() : 0L;
            newValue = current + delta;
        }
        else
            newValue = delta;

        var valueBytes = BitConverter.GetBytes(newValue);
        var valueOffset = WriteSetRecordNoLock(key, valueBytes, expiresAt);
        _data[key] = new KvEntry { ValueOffset = valueOffset, ValueLength = 8, ExpiresAt = expiresAt };
        TryAutoCompactNoLock();
        return newValue;
    }
}
```

---

## 5. 持久化策略

### 5.1 WAL 模式

通过 `DbOptions.WalMode` 配置刷盘策略：

| 模式 | 行为 | 适用场景 |
|------|------|---------|
| **Full** | 每次写操作同步刷盘 `Flush(true)` | 强一致性需求 |
| **Normal** | 定时刷盘（1 秒周期） | 默认模式，平衡性能与持久性 |
| **None** | 不主动刷盘，由 OS 管理 | 最高写入性能，可能丢失数据 |

### 5.2 启动恢复

Bitcask 模型启动时扫描数据文件重建内存索引，**不加载值数据**，内存占用极低：

```
文件大小判断
  ├── > 64KB → MemoryMappedFile 扫描（零拷贝，大文件高效）
  └── ≤ 64KB → FileStream 读取（小文件开销低）
      ↓
顺序回放记录
  ├── RecordType_Set → CRC 校验 → 跳过过期键 → 记录 (ValueOffset, ValueLength, ExpiresAt) 到索引
  ├── RecordType_Delete → CRC 校验 → 从索引移除
  └── RecordType_Clear → 清空索引
```

恢复时仅建立键到文件偏移的映射，值在实际 Get 时才从磁盘读取。

### 5.3 数据压缩（Compact）

随着删除和更新操作积累，数据文件会产生无效记录。`Compact()` 采用**临时文件替换策略**，逐条从旧文件读取存活条目写入临时文件，完成后替换原文件，同时重建内存索引：

```csharp
public void Compact()
{
    lock (_writeLock)
    {
        var tempPath = _filePath + ".compact.tmp";
        using var tempStream = new FileStream(tempPath, ...);
        WriteFileHeader(tempStream);

        foreach (var kvp in _data)
        {
            if (kvp.Value.IsExpired()) continue;
            using var pk = ReadValueFromDiskNoLock(kvp.Value);      // 从旧文件读值
            var value = pk != null ? pk.GetSpan().ToArray() : [];
            WriteSetRecordToStream(tempStream, kvp.Key, value, ...); // 写入临时文件
        }

        // 关闭旧文件 → 删除旧文件 → 重命名临时文件 → 重建索引
        _fileStream.Dispose();
        File.Delete(_filePath);
        File.Move(tempPath, _filePath);
        _fileStream = new FileStream(_filePath, ...);
    }
}
```

**优势**：每次仅一条记录在内存中，适合大文件场景，避免一次性加载全部值到内存。

#### 自动压缩

通过 `DbOptions.KvCompactRatio`（默认 5.0）控制自动触发：当写入记录总数 ÷ 存活键数超过比率阈值，且存活键数 ≥ 100 时自动执行 Compact。设为 0 禁用自动压缩。

---

## 6. TTL 清理

### 双重策略

| 策略 | 触发时机 | 说明 |
|------|---------|------|
| **惰性删除** | Get / TryGet / Exists 时 | 发现过期 → 从字典移除并返回 null |
| **主动清理** | 调用 `CleanupExpired()` | 遍历全部键，批量删除过期条目 |

启动恢复时也跳过已过期的记录，不加载到内存。

---

## 7. ICache 集成

`NovaCache` 是 KvStore 的 ICache 适配器，桥接 NewLife.Core 的缓存接口。由于 KvStore 的 Get 返回池化数据包（`IOwnerPacket`），NovaCache 负责反序列化并释放资源：

```csharp
public class NovaCache : ICache
{
    private readonly KvStore _kvStore;

    // ICache 方法映射到 KvStore，自动处理 IOwnerPacket 的生命周期
    public T Get<T>(String key)
    {
        using var pk = _kvStore.Get(key);
        return Deserialize<T>(pk);
    }
    public Boolean Set<T>(String key, T value, Int32 expire = -1) => ...;
    public Int64 Increment(String key, Int64 value) => _kvStore.Inc(key, value);
    public Double Increment(String key, Double value) => _kvStore.IncDouble(key, value);
    ...
}
```

---

## 8. 应用场景

### 8.1 嵌入式缓存

```csharp
var db = new EmbeddedDatabase(new DbOptions { DataDir = "/data/mydb" });
var cache = db.GetCache("session");
cache.Set("user:123", userData, 7200);
var user = cache.Get<User>("user:123");
```

### 8.2 分布式锁

```csharp
var store = new KvStore(options, "/data/mydb/lock.kvd");
if (store.Add("lock:order_123", lockValue, TimeSpan.FromSeconds(30)))
{
    try { ProcessOrder(123); }
    finally { store.Delete("lock:order_123"); }
}
```

### 8.3 计数器

```csharp
var store = new KvStore(options, "/data/mydb/counter.kvd");
store.Inc("page_views:home");
store.Inc("page_views:home", 10);
store.IncDouble("score:user:123", 3.14);
```

### 8.4 批量操作

```csharp
// 批量设置
store.SetAll(new Dictionary<String, Byte[]?>
{
    ["key1"] = value1,
    ["key2"] = value2,
    ["key3"] = value3,
}, TimeSpan.FromHours(1));

// 批量获取
var results = store.GetAll(new[] { "key1", "key2", "key3" });

// 模式搜索
var keys = store.Search("user:*");
store.DeleteByPattern("temp:*");
```

---

## 9. 设计决策

| # | 决策 | 要点 |
|---|------|------|
| D1 | Bitcask 模型 | 内存仅存索引（~20B/key），值留磁盘按需读取，百万键仅占 ~20MB |
| D2 | ConcurrentDictionary | 读操作无锁，写操作仅在需要原子性时加锁 |
| D3 | 一表一文件 | 每个 KV 表独立 .kvd 文件，互不干扰，支持独立生命周期管理 |
| D4 | 二进制计数器 | Inc/IncDouble 值以二进制 Int64/Double 存储（8 字节），高效紧凑 |
| D5 | MMF 恢复加速 | 大文件（>64KB）使用 MemoryMappedFile 零拷贝读取，跨框架兼容 |
| D6 | WAL 模式可配 | Full/Normal/None 三档，用户按场景选择一致性与性能的平衡点 |
| D7 | 惰性 + 主动 TTL | 读时惰性删除保证不返回过期数据；主动清理回收内存占用 |
| D8 | 池化返回值 | Get 返回 IOwnerPacket（池化数据包），用完 Dispose 归还池中，减少 GC 压力 |
| D9 | 临时文件 Compact | Compact 写入临时文件后替换原文件，逐条读写避免大内存峰值 |
| D10 | 自动压缩 | KvCompactRatio 控制自动触发，存活键 ≥ 100 时生效，避免小数据量频繁压缩 |

---

## 10. 类关系

```
KvStore (Engine/KV/KvStore.cs + KvStore.Persist.cs, ~1,070 行)
  ├── KvEntry (数据结构：ValueOffset, ValueLength, ExpiresAt)
  ├── KvRecordType (枚举：Set, Delete, Clear)
  ├── DbOptions (配置：WalMode, DefaultKvTtl, KvCompactRatio)
  ├── FileHeader (统一文件头，32 字节)
  └── CRC32 (NewLife.Security.Crc32，记录校验)

NovaCache (Caching/NovaCache.cs)
  └── KvStore (ICache 适配器，处理 IOwnerPacket 生命周期)

EmbeddedDatabase (Client/EmbeddedDatabase.cs)
  └── KvStore (每个 KV 表 = 一个 KvStore 实例 + .kvd 文件)

NovaServer (Server/NovaServer.cs)
  └── KvStore (default.kvd 默认 KV 表)
```

---

（完）
