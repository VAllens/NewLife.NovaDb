using NewLife.Caching;
using NewLife.NovaDb.Client;
using NewLife.NovaDb.Core;
using NewLife.NovaDb.Engine.Flux;
using NewLife.NovaDb.Engine.KV;
using NewLife.NovaDb.Queues;

namespace NewLife.NovaDb.Caching;

/// <summary>Nova 缓存架构服务。提供基础缓存及队列服务</summary>
/// <remarks>
/// 参考 RedisCacheProvider 实现，根据连接字符串自动识别嵌入模式还是网络服务模式。
/// 嵌入模式直接使用本地 KvStore/FluxEngine 引擎；网络模式通过 NovaClient 远程操作。
/// </remarks>
public class NovaCacheProvider : CacheProvider
{
    #region 属性
    private NovaCache? _novaCache;
    private FluxEngine? _fluxEngine;

    /// <summary>Flux 引擎（队列功能）</summary>
    public FluxEngine? FluxEngine
    {
        get => _fluxEngine;
        set
        {
            _fluxEngine = value;
            _novaCache?.FluxEngine = value;
        }
    }
    #endregion

    #region 构造
    /// <summary>使用连接字符串创建 NovaCacheProvider</summary>
    /// <param name="connectionString">连接字符串。嵌入模式：Data Source=../data；网络模式：Server=localhost;Port=3306</param>
    public NovaCacheProvider(String connectionString)
    {
        Init(connectionString);
    }
    #endregion

    #region 方法
    /// <summary>根据连接字符串初始化</summary>
    /// <param name="connectionString">连接字符串</param>
    private void Init(String connectionString)
    {
        if (String.IsNullOrEmpty(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        var csb = new NovaConnectionStringBuilder(connectionString);
        if (csb.IsEmbedded)
        {
            // 嵌入模式
            var dataSource = csb.DataSource!;
            var kvStore = new KvStore(null, dataSource);

            // 创建 Flux 引擎用于队列功能
            var mqPath = Path.Combine(dataSource, "mq");
            _fluxEngine = new FluxEngine(mqPath, new DbOptions());

            _novaCache = new NovaCache(kvStore) { FluxEngine = _fluxEngine };
            Cache = _novaCache;
        }
        else
        {
            // 网络服务模式
            var server = csb.Server ?? "localhost";
            var port = csb.Port;
            var uri = $"tcp://{server}:{port}";

            var client = new NovaClient(uri);
            client.Open();

            _novaCache = new NovaCache(client);
            Cache = _novaCache;
        }
    }

    /// <summary>获取队列</summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="topic">主题</param>
    /// <param name="group">消费组</param>
    /// <returns>队列实例</returns>
    public override IProducerConsumer<T> GetQueue<T>(String topic, String? group = null)
    {
        if (_fluxEngine != null)
        {
            var queue = new NovaQueue<T>(_fluxEngine, topic);
            if (!String.IsNullOrEmpty(group))
                queue.SetGroup(group!);
            return queue;
        }

        return base.GetQueue<T>(topic, group);
    }
    #endregion
}
