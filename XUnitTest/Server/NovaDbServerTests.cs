using System;
using System.Text;
using NewLife.NovaDb.Server;
using Xunit;

namespace XUnitTest.Server;

/// <summary>NovaDb 服务器单元测试</summary>
public class NovaDbServerTests : IDisposable
{
    private readonly NovaDbServer _server;

    public NovaDbServerTests()
    {
        _server = new NovaDbServer(0);
    }

    public void Dispose() => _server.Dispose();

    [Fact(DisplayName = "测试创建服务器")]
    public void TestCreateServer()
    {
        Assert.NotNull(_server);
        Assert.Equal(0, _server.Port);
        Assert.False(_server.IsRunning);
        Assert.Equal(0, _server.SessionCount);
    }

    [Fact(DisplayName = "测试启动和停止服务器")]
    public void TestStartAndStop()
    {
        _server.Start();
        Assert.True(_server.IsRunning);

        _server.Stop();
        Assert.False(_server.IsRunning);
    }

    [Fact(DisplayName = "测试创建和获取会话")]
    public void TestCreateAndGetSession()
    {
        var session = _server.CreateSession();

        Assert.NotNull(session);
        Assert.NotEmpty(session.SessionId);
        Assert.Equal(1, _server.SessionCount);

        var found = _server.GetSession(session.SessionId);
        Assert.NotNull(found);
        Assert.Equal(session.SessionId, found!.SessionId);
    }

    [Fact(DisplayName = "测试获取不存在的会话")]
    public void TestGetNonExistentSession()
    {
        var found = _server.GetSession("non-existent-id");
        Assert.Null(found);
    }

    [Fact(DisplayName = "测试移除会话")]
    public void TestRemoveSession()
    {
        var session = _server.CreateSession();
        Assert.Equal(1, _server.SessionCount);

        var removed = _server.RemoveSession(session.SessionId);
        Assert.True(removed);
        Assert.Equal(0, _server.SessionCount);

        // 移除不存在的会话
        removed = _server.RemoveSession("non-existent-id");
        Assert.False(removed);
    }

    [Fact(DisplayName = "测试会话过期")]
    public void TestSessionExpiration()
    {
        var session = new NovaDbSession
        {
            TimeoutSeconds = 0
        };

        // 超时为 0 秒，应立即过期
        Assert.True(session.IsExpired());
    }

    [Fact(DisplayName = "测试会话刷新")]
    public void TestSessionTouch()
    {
        var session = new NovaDbSession
        {
            TimeoutSeconds = 300
        };

        var before = session.LastActiveAt;
        System.Threading.Thread.Sleep(10);
        session.Touch();
        var after = session.LastActiveAt;

        Assert.True(after >= before);
        Assert.False(session.IsExpired());
    }

    [Fact(DisplayName = "测试处理心跳请求")]
    public void TestHandlePingRequest()
    {
        var session = _server.CreateSession();
        var header = new ProtocolHeader
        {
            RequestType = RequestType.Ping,
            SequenceId = 1
        };

        var response = _server.HandleRequest(header, [], session);
        Assert.NotNull(response);
        Assert.True(response.Length >= ProtocolHeader.HeaderSize);

        var respHeader = ProtocolHeader.FromBytes(response);
        Assert.Equal(ResponseStatus.Ok, respHeader.Status);
        Assert.Equal(RequestType.Ping, respHeader.RequestType);
        Assert.Equal((UInt32)1, respHeader.SequenceId);
    }

    [Fact(DisplayName = "测试处理握手请求")]
    public void TestHandleHandshakeRequest()
    {
        var session = _server.CreateSession();
        Assert.False(session.IsAuthenticated);

        var header = new ProtocolHeader
        {
            RequestType = RequestType.Handshake,
            SequenceId = 1
        };

        var response = _server.HandleRequest(header, [], session);
        Assert.NotNull(response);
        Assert.True(session.IsAuthenticated);

        var respHeader = ProtocolHeader.FromBytes(response);
        Assert.Equal(ResponseStatus.Ok, respHeader.Status);

        // 响应负载应包含 SessionId
        var payload = new Byte[respHeader.PayloadLength];
        Array.Copy(response, ProtocolHeader.HeaderSize, payload, 0, respHeader.PayloadLength);
        var sessionId = Encoding.UTF8.GetString(payload);
        Assert.Equal(session.SessionId, sessionId);
    }

    [Fact(DisplayName = "测试处理事务请求")]
    public void TestHandleTransactionRequests()
    {
        var session = _server.CreateSession();
        var types = new[] { RequestType.BeginTx, RequestType.CommitTx, RequestType.RollbackTx };

        foreach (var reqType in types)
        {
            var header = new ProtocolHeader
            {
                RequestType = reqType,
                SequenceId = 1
            };

            var response = _server.HandleRequest(header, [], session);
            var respHeader = ProtocolHeader.FromBytes(response);
            Assert.Equal(ResponseStatus.Ok, respHeader.Status);
        }
    }

    [Fact(DisplayName = "测试清理过期会话")]
    public void TestCleanupExpiredSessions()
    {
        // 创建多个会话
        var s1 = _server.CreateSession();
        var s2 = _server.CreateSession();
        var s3 = _server.CreateSession();

        Assert.Equal(3, _server.SessionCount);

        // 设置 s1 和 s2 为即将过期
        s1.TimeoutSeconds = 0;
        s2.TimeoutSeconds = 0;

        // 清理过期会话
        var cleaned = _server.CleanupExpiredSessions();
        Assert.Equal(2, cleaned);
        Assert.Equal(1, _server.SessionCount);

        // s3 应该还在
        Assert.NotNull(_server.GetSession(s3.SessionId));
    }

    [Fact(DisplayName = "测试停止服务器清理会话")]
    public void TestStopClearsSessions()
    {
        _server.Start();
        _server.CreateSession();
        _server.CreateSession();

        Assert.Equal(2, _server.SessionCount);

        _server.Stop();
        Assert.Equal(0, _server.SessionCount);
        Assert.False(_server.IsRunning);
    }

    [Fact(DisplayName = "测试处理执行请求返回未实现")]
    public void TestHandleExecuteReturnsNotImplemented()
    {
        var session = _server.CreateSession();
        var header = new ProtocolHeader
        {
            RequestType = RequestType.Execute,
            SequenceId = 1
        };

        var response = _server.HandleRequest(header, [], session);
        var respHeader = ProtocolHeader.FromBytes(response);
        Assert.Equal(ResponseStatus.Error, respHeader.Status);
    }
}
