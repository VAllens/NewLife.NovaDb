using NewLife.Log;
using NewLife.Remoting;

namespace NewLife.NovaDb.Server;

/// <summary>NovaDb TCP 服务器，基于 Remoting 的 ApiServer 实现 RPC 通信</summary>
public class NovaDbServer : DisposeBase
{
    private ApiServer? _server;
    private readonly Int32 _port;

    /// <summary>端口</summary>
    public Int32 Port => _server?.Port ?? _port;

    /// <summary>是否运行中</summary>
    public Boolean IsRunning => _server?.Active ?? false;

    /// <summary>内部 ApiServer 实例</summary>
    public ApiServer? Server => _server;

    /// <summary>创建服务器实例</summary>
    /// <param name="port">监听端口</param>
    public NovaDbServer(Int32 port = 3306)
    {
        _port = port;
    }

    /// <summary>启动服务器</summary>
    public void Start()
    {
        if (_server != null && _server.Active) return;

        var server = new ApiServer(_port)
        {
            Log = XTrace.Log,
        };

        // 注册 NovaDb 业务控制器
        server.Register<NovaController>();

        server.Start();
        _server = server;
    }

    /// <summary>停止服务器</summary>
    /// <param name="reason">停止原因</param>
    public void Stop(String? reason = null)
    {
        _server?.Stop(reason ?? "NovaDbServer.Stop");
        _server = null;
    }

    /// <summary>释放资源</summary>
    /// <param name="disposing">是否由 Dispose 调用</param>
    protected override void Dispose(Boolean disposing)
    {
        base.Dispose(disposing);

        _server?.Stop(disposing ? "Dispose" : "GC");
        _server.TryDispose();
        _server = null;
    }
}
