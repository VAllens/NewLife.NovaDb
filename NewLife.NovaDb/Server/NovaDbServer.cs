using System.Text;
using NewLife.NovaDb.Core;

namespace NewLife.NovaDb.Server;

/// <summary>NovaDb TCP 服务器</summary>
public class NovaDbServer : IDisposable
{
    private readonly Int32 _port;
    private readonly Dictionary<String, NovaDbSession> _sessions = [];
    private readonly Object _lock = new();
    private Boolean _isRunning;
    private Boolean _disposed;

    /// <summary>端口</summary>
    public Int32 Port => _port;

    /// <summary>是否运行中</summary>
    public Boolean IsRunning => _isRunning;

    /// <summary>活跃会话数</summary>
    public Int32 SessionCount
    {
        get
        {
            lock (_lock)
            {
                return _sessions.Count;
            }
        }
    }

    /// <summary>最大会话数，默认 10000</summary>
    public Int32 MaxSessions { get; set; } = 10000;

    /// <summary>创建服务器实例</summary>
    /// <param name="port">监听端口，默认 3306</param>
    public NovaDbServer(Int32 port = 3306)
    {
        _port = port;
    }

    /// <summary>启动服务器</summary>
    public void Start()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(NovaDbServer));

        _isRunning = true;
    }

    /// <summary>停止服务器</summary>
    public void Stop()
    {
        _isRunning = false;

        lock (_lock)
        {
            _sessions.Clear();
        }
    }

    /// <summary>创建会话</summary>
    /// <returns>新创建的会话</returns>
    public NovaDbSession CreateSession()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(NovaDbServer));

        var session = new NovaDbSession();

        lock (_lock)
        {
            if (_sessions.Count >= MaxSessions)
                throw new NovaDbException(ErrorCode.ConnectionFailed, $"Maximum sessions ({MaxSessions}) exceeded");

            _sessions[session.SessionId] = session;
        }

        return session;
    }

    /// <summary>获取会话</summary>
    /// <param name="sessionId">会话 ID</param>
    /// <returns>会话实例，未找到返回 null</returns>
    public NovaDbSession? GetSession(String sessionId)
    {
        if (sessionId == null) throw new ArgumentNullException(nameof(sessionId));

        lock (_lock)
        {
            return _sessions.TryGetValue(sessionId, out var session) ? session : null;
        }
    }

    /// <summary>移除会话</summary>
    /// <param name="sessionId">会话 ID</param>
    /// <returns>是否成功移除</returns>
    public Boolean RemoveSession(String sessionId)
    {
        if (sessionId == null) throw new ArgumentNullException(nameof(sessionId));

        lock (_lock)
        {
            return _sessions.Remove(sessionId);
        }
    }

    /// <summary>处理请求</summary>
    /// <param name="header">协议头</param>
    /// <param name="payload">负载数据</param>
    /// <param name="session">客户端会话</param>
    /// <returns>响应字节数组</returns>
    public Byte[] HandleRequest(ProtocolHeader header, Byte[] payload, NovaDbSession session)
    {
        if (header == null) throw new ArgumentNullException(nameof(header));
        if (session == null) throw new ArgumentNullException(nameof(session));

        session.Touch();

        var response = new ProtocolHeader
        {
            Version = header.Version,
            RequestType = header.RequestType,
            SequenceId = header.SequenceId,
            Status = ResponseStatus.Ok
        };

        Byte[] responsePayload;

        switch (header.RequestType)
        {
            case RequestType.Ping:
                responsePayload = [];
                break;

            case RequestType.Handshake:
                session.IsAuthenticated = true;
                responsePayload = Encoding.UTF8.GetBytes(session.SessionId);
                break;

            case RequestType.Execute:
            case RequestType.Query:
            case RequestType.Fetch:
            case RequestType.BeginTx:
            case RequestType.CommitTx:
            case RequestType.RollbackTx:
                // 验证认证状态
                if (!session.IsAuthenticated)
                {
                    response.Status = ResponseStatus.Error;
                    responsePayload = Encoding.UTF8.GetBytes("Authentication required");
                    break;
                }

                if (header.RequestType is RequestType.Execute or RequestType.Query or RequestType.Fetch)
                {
                    response.Status = ResponseStatus.Error;
                    responsePayload = Encoding.UTF8.GetBytes("Not yet implemented");
                }
                else
                {
                    responsePayload = [];
                }
                break;

            case RequestType.Close:
                responsePayload = [];
                break;

            default:
                response.Status = ResponseStatus.Error;
                responsePayload = Encoding.UTF8.GetBytes("Unknown request type");
                break;
        }

        response.PayloadLength = responsePayload.Length;

        var headerBytes = response.ToBytes();
        var result = new Byte[headerBytes.Length + responsePayload.Length];
        Array.Copy(headerBytes, 0, result, 0, headerBytes.Length);
        if (responsePayload.Length > 0)
            Array.Copy(responsePayload, 0, result, headerBytes.Length, responsePayload.Length);

        return result;
    }

    /// <summary>清理过期会话</summary>
    /// <returns>清理的会话数量</returns>
    public Int32 CleanupExpiredSessions()
    {
        lock (_lock)
        {
            var expired = new List<String>();
            foreach (var kvp in _sessions)
            {
                if (kvp.Value.IsExpired())
                    expired.Add(kvp.Key);
            }

            foreach (var key in expired)
            {
                _sessions.Remove(key);
            }

            return expired.Count;
        }
    }

    /// <summary>释放资源</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        Stop();
    }
}
