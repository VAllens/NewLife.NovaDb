## SQL 函数支持清单

以下按功能分类列出 SQL-92 及常见扩展函数，包含优先级标注（P0/P1/P2）、简要语义说明与兼容性提示。此文档用于实现与测试参考，需求层仅保留类别级别说明。

优先级约定：
- P0（必须）：v1 实现，覆盖 80% 业务场景。
- P1（高）：v1 建议实现，覆盖 95% 业务场景。
- P2（可选）：v2+ 按需实现。

---

#### 1 聚合函数

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `COUNT(*)` / `COUNT(column)` | **P0** | 行计数，支持 `DISTINCT` |
| `SUM(column)` | **P0** | 数值求和 |
| `AVG(column)` | **P0** | 平均值 |
| `MIN(column)` / `MAX(column)` | **P0** | 最小/最大值 |
| `STRING_AGG(column, sep)` | **P1** | 字符串聚合（拼接） |
| `GROUP_CONCAT(column)` | **P1** | MySQL 风格字符串聚合 |
| `STDDEV(column)` / `VARIANCE(column)` | **P2** | 标准差/方差（统计场景） |

---

#### 2 字符串函数

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `CONCAT(str1, str2, ...)` | **P0** | 字符串拼接 |
| `LENGTH(str)` / `LEN(str)` | **P0** | 字符串长度（字节数） |
| `SUBSTRING(str, pos, len)` | **P0** | 子串提取（`pos` 从 1 开始） |
| `UPPER(str)` / `LOWER(str)` | **P0** | 大小写转换 |
| `TRIM(str)` / `LTRIM(str)` / `RTRIM(str)` | **P0** | 去除空白 |
| `REPLACE(str, from, to)` | **P0** | 字符串替换 |
| `LEFT(str, n)` / `RIGHT(str, n)` | **P0** | 左/右截取 |
| `CHARINDEX(substr, str)` / `INSTR(substr, str)` | **P1** | 查找子串位置（返回索引，不存在返回 0） |
| `REVERSE(str)` | **P2** | 反转字符串 |
| `LPAD(str, len, pad)` / `RPAD(str, len, pad)` | **P2** | 左/右填充至指定长度 |
| `FORMAT(value, format)` | **不支持** | 复杂格式化（应用层实现） |
| `SOUNDEX(str)` / `DIFFERENCE(str1, str2)` | **不支持** | 音似匹配（冷门） |

---

#### 3 数值函数

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `ABS(n)` | **P0** | 绝对值 |
| `ROUND(n, decimals)` | **P0** | 四舍五入（`decimals` 为 0 时取整） |
| `CEILING(n)` / `FLOOR(n)` | **P0** | 向上/向下取整 |
| `POWER(base, exp)` / `SQRT(n)` | **P1** | 幂运算/平方根 |
| `MOD(n, m)` / `%` | **P0** | 取模 |
| `RAND()` / `RANDOM()` | **P1** | 随机数（0-1） |
| `SIGN(n)` | **P1** | 符号（-1/0/1） |
| `TRUNCATE(n, decimals)` | **P1** | 截断（不四舍五入） |
| `PI()` | **P2** | 圆周率常数 |
| `EXP(n)` / `LOG(n)` / `LOG10(n)` | **P2** | 指数/自然对数/常用对数 |
| `SIN(n)` / `COS(n)` / `TAN(n)` | **不支持** | 三角函数（冷门） |
| `ASIN(n)` / `ACOS(n)` / `ATAN(n)` | **不支持** | 反三角函数 |

---

#### 4 日期时间函数

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `NOW()` / `GETDATE()` / `CURRENT_TIMESTAMP` | **P0** | 当前时间 |
| `YEAR(date)` / `MONTH(date)` / `DAY(date)` | **P0** | 提取年/月/日 |
| `HOUR(time)` / `MINUTE(time)` / `SECOND(time)` | **P0** | 提取时/分/秒 |
| `DATEDIFF(date1, date2)` | **P0** | 日期差（天数） |
| `DATEADD(interval, n, date)` / `DATE_ADD(date, INTERVAL n unit)` | **P0** | 日期加减（`unit`: YEAR/MONTH/DAY/HOUR 等） |
| `DATEPART(part, date)` | **P1** | 提取日期部分（通用版本，`part`: YEAR/MONTH/DAY 等） |
| `DATE_FORMAT(date, format)` | **P1** | 格式化输出（如 `'%Y-%m-%d'`） |
| `WEEKDAY(date)` / `DAYOFWEEK(date)` | **P1** | 星期几（1-7） |
| `LAST_DAY(date)` | **P2** | 月末日期 |
| `TIMESTAMPDIFF(unit, date1, date2)` | **P2** | 时间戳差（按单位） |
| `TIMESTAMPADD(unit, n, date)` | **P2** | 时间戳加减 |

---

#### 5 类型转换函数

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `CAST(value AS type)` | **P0** | 标准类型转换（如 `CAST(123 AS STRING)`） |
| `CONVERT(type, value)` | **P0** | SQL Server 风格转换 |
| `COALESCE(v1, v2, ...)` | **P0** | 返回第一个非 NULL 值 |
| `ISNULL(value, default)` / `IFNULL(value, default)` | **P0** | NULL 替换 |
| `NULLIF(v1, v2)` | **P1** | 相等时返回 NULL，否则返回 v1 |

---

#### 6 条件函数

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `CASE WHEN ... THEN ... ELSE ... END` | **P0** | 条件表达式（标准 SQL） |
| `IF(condition, true_val, false_val)` | **P0** | MySQL 风格条件（三元运算符） |
| `IIF(condition, true_val, false_val)` | **P1** | SQL Server 风格条件 |

---

#### 7 系统/元数据函数

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `DATABASE()` / `CURRENT_DATABASE()` | **P1** | 当前数据库名 |
| `VERSION()` | **P1** | 数据库版本（如 `NovaDb 1.0.2024.1201`） |
| `ROW_COUNT()` / `@@ROWCOUNT` | **P1** | 上一条 DML 影响的行数 |
| `LAST_INSERT_ID()` / `@@IDENTITY` | **P1** | 最后插入的自增主键 ID |
| `USER()` / `CURRENT_USER()` | **P2** | 当前用户（v1 无权限系统，返回默认值 `'nova'`） |
| `CONNECTION_ID()` | **P2** | 当前连接 ID |

---

#### 8 地理/向量函数（NovaDb 扩展）

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `DISTANCE(point1, point2)` | **P1** | 地理距离（米），Haversine 公式 |
| `WITHIN_RADIUS(point, center, radius)` | **P1** | 判断点是否在圆形范围内 |
| `COSINE_SIMILARITY(vec1, vec2)` | **P1** | 向量余弦相似度 |
| `EUCLIDEAN_DISTANCE(vec1, vec2)` | **P1** | 向量欧氏距离 |
| `DOT_PRODUCT(vec1, vec2)` | **P2** | 向量点积 |

---

#### 9 加密/哈希函数

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `MD5(str)` | **P1** | MD5 哈希（32 字符十六进制） |
| `SHA1(str)` | **P1** | SHA1 哈希（40 字符十六进制） |
| `SHA2(str, bits)` | **P1** | SHA2 哈希（`bits`: 256/384/512） |
| `ENCRYPT(str, key)` / `AES_ENCRYPT(str, key)` | **不支持** | 加密（应用层实现） |

---

#### 10 正则表达式函数

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `REGEXP(str, pattern)` / `RLIKE(str, pattern)` | **不支持** | 正则匹配（性能开销大） |
| `REGEXP_REPLACE(str, pattern, replacement)` | **不支持** | 正则替换 |

---

## 使用说明与后续工作

- 每个函数条目在实现时需补充：语法、NULL 处理、返回类型、示例、边界/性能说明与测试用例 ID。
- 建议按优先级分阶段实现：v1 优先实现所有 P0，次阶段实现 P1，P2 列入长期计划。
- 扩展函数（地理/向量/JSON/全文）在独立模块中实现并以插件化方式启用，便于回滚与性能隔离。
