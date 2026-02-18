using System;
using System.Text;
using System.Threading;
using NewLife.NovaDb.Engine.KV;
using Xunit;

namespace XUnitTest.Engine.KV;

/// <summary>KV 存储引擎单元测试</summary>
public class KvStoreTests
{
    [Fact(DisplayName = "测试设置和获取值")]
    public void TestSetAndGet()
    {
        var store = new KvStore();
        var value = Encoding.UTF8.GetBytes("hello");

        store.Set("key1", value);

        var result = store.Get("key1");
        Assert.NotNull(result);
        Assert.Equal(value, result);
    }

    [Fact(DisplayName = "测试删除键")]
    public void TestDelete()
    {
        var store = new KvStore();
        store.Set("key1", [1, 2, 3]);

        Assert.True(store.Delete("key1"));
        Assert.False(store.Delete("key1"));
        Assert.False(store.Delete("nonexistent"));
    }

    [Fact(DisplayName = "测试键是否存在")]
    public void TestExists()
    {
        var store = new KvStore();
        store.Set("key1", [1, 2, 3]);

        Assert.True(store.Exists("key1"));
        Assert.False(store.Exists("key2"));
    }

    [Fact(DisplayName = "测试字符串便捷方法")]
    public void TestStringConvenience()
    {
        var store = new KvStore();
        store.SetString("name", "NovaDb");

        var result = store.GetString("name");
        Assert.Equal("NovaDb", result);

        Assert.Null(store.GetString("missing"));
    }

    [Fact(DisplayName = "测试 TTL 过期")]
    public void TestTtlExpiration()
    {
        var store = new KvStore();
        store.Set("key1", [1, 2, 3], TimeSpan.FromMilliseconds(10));

        // 未过期时可以获取
        Assert.NotNull(store.Get("key1"));

        // 等待过期
        Thread.Sleep(50);

        Assert.Null(store.Get("key1"));
        Assert.False(store.Exists("key1"));
    }

    [Fact(DisplayName = "测试惰性删除")]
    public void TestLazyDeletion()
    {
        var store = new KvStore();
        store.Set("key1", [1, 2, 3], TimeSpan.FromMilliseconds(10));

        Thread.Sleep(50);

        // Get 触发惰性删除
        Assert.Null(store.Get("key1"));

        // 验证条目已被移除（Count 不再包含该键）
        Assert.Equal(0, store.Count);
    }

    [Fact(DisplayName = "测试后台清理")]
    public void TestCleanupExpired()
    {
        var store = new KvStore();
        store.Set("key1", [1], TimeSpan.FromMilliseconds(10));
        store.Set("key2", [2], TimeSpan.FromMilliseconds(10));
        store.Set("key3", [3]); // 永不过期

        Thread.Sleep(50);

        var removed = store.CleanupExpired();
        Assert.Equal(2, removed);
        Assert.Equal(1, store.Count);
        Assert.True(store.Exists("key3"));
    }

    [Fact(DisplayName = "测试更新 TTL")]
    public void TestSetExpiration()
    {
        var store = new KvStore();
        store.Set("key1", [1, 2, 3]);

        // 设置 TTL
        Assert.True(store.SetExpiration("key1", TimeSpan.FromHours(1)));

        var expiration = store.GetExpiration("key1");
        Assert.NotNull(expiration);

        // 不存在的键返回 false
        Assert.False(store.SetExpiration("missing", TimeSpan.FromHours(1)));
    }

    [Fact(DisplayName = "测试获取过期时间")]
    public void TestGetExpiration()
    {
        var store = new KvStore();

        // 无 TTL
        store.Set("key1", [1]);
        Assert.Null(store.GetExpiration("key1"));

        // 有 TTL
        store.Set("key2", [2], TimeSpan.FromHours(1));
        var expiration = store.GetExpiration("key2");
        Assert.NotNull(expiration);
        Assert.True(expiration.Value > DateTime.UtcNow);

        // 不存在的键
        Assert.Null(store.GetExpiration("missing"));
    }

    [Fact(DisplayName = "测试覆盖已有值")]
    public void TestOverwriteValue()
    {
        var store = new KvStore();
        store.Set("key1", [1, 2, 3]);
        store.Set("key1", [4, 5, 6]);

        var result = store.Get("key1");
        Assert.NotNull(result);
        Assert.Equal(new Byte[] { 4, 5, 6 }, result);
        Assert.Equal(1, store.Count);
    }

    [Fact(DisplayName = "测试获取所有键")]
    public void TestGetAllKeys()
    {
        var store = new KvStore();
        store.Set("key1", [1]);
        store.Set("key2", [2], TimeSpan.FromMilliseconds(10));
        store.Set("key3", [3]);

        Thread.Sleep(50);

        var keys = store.GetAllKeys();
        Assert.Equal(2, keys.Count);
        Assert.Contains("key1", keys);
        Assert.Contains("key3", keys);
        Assert.DoesNotContain("key2", keys);
    }

    [Fact(DisplayName = "测试计数属性")]
    public void TestCountProperty()
    {
        var store = new KvStore();
        Assert.Equal(0, store.Count);

        store.Set("key1", [1]);
        store.Set("key2", [2]);
        Assert.Equal(2, store.Count);

        store.Set("key3", [3], TimeSpan.FromMilliseconds(10));
        Thread.Sleep(50);

        // 过期的不计入
        Assert.Equal(2, store.Count);
    }

    [Fact(DisplayName = "测试空键抛出异常")]
    public void TestEmptyKeyThrows()
    {
        var store = new KvStore();

        Assert.Throws<ArgumentException>(() => store.Set(null!, [1]));
        Assert.Throws<ArgumentException>(() => store.Set("", [1]));
        Assert.Throws<ArgumentException>(() => store.Get(null!));
        Assert.Throws<ArgumentException>(() => store.Get(""));
        Assert.Throws<ArgumentException>(() => store.Delete(null!));
        Assert.Throws<ArgumentException>(() => store.Delete(""));
        Assert.Throws<ArgumentException>(() => store.Exists(null!));
        Assert.Throws<ArgumentException>(() => store.Exists(""));
    }

    [Fact(DisplayName = "测试空值可存储")]
    public void TestNullValueCanBeStored()
    {
        var store = new KvStore();
        store.Set("key1", null);

        Assert.True(store.Exists("key1"));

        var result = store.Get("key1");
        Assert.Null(result);
        Assert.Equal(1, store.Count);
    }
}
