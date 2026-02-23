# SQL 函数支持清单

## 文档概述

本文档按功能分类列出 NewLife.NovaDb 支持的 SQL 函数，包含优先级标注（P0/P1/P2）、语义说明、NULL 处理、返回类型、示例与兼容性提示。

### 优先级约定

- **P0（必须）**：v1 必须实现，覆盖 80% 业务场景，无差别支持
- **P1（高）**：v1 建议实现，覆盖 95% 业务场景，可部分回避
- **P2（可选）**：v2+ 按需实现，属于增强功能

### 兼容性说明

- **来源标注**：标注函数来自 SQL-92 标准、MySQL、PostgreSQL、SQL Server 等，便于用户理解差异
- **NULL 传播**：特别注明 NULL 处理行为（多数聚合函数 NULL 不计，标量函数 NULL 传播）
- **返回类型**：明确指定返回类型，便于驱动程序与 ORM 适配

---

## SQL 函数支持清单

### 1. 聚合函数（Aggregate Functions）

聚合函数用于 `GROUP BY` 子句或窗口函数中，对分组内的多行进行计算。

| 函数 | 优先级 | 语法 | NULL 处理 | 返回类型 | 说明 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `COUNT(*)` | **P0** | `COUNT(*)` | 计数所有行，包括 NULL | INT | 返回结果集的总行数 |
| `COUNT(column)` | **P0** | `COUNT(column)` | NULL 不计 | INT | 返回非 NULL 值的行数 |
| `COUNT(DISTINCT column)` | **P0** | `COUNT(DISTINCT column)` | NULL 不计 | INT | 返回不同非 NULL 值的个数 |
| `SUM(column)` | **P0** | `SUM(column)` | NULL 不计，全为 NULL 返回 NULL | DECIMAL/LONG | 数值求和 |
| `AVG(column)` | **P0** | `AVG(column)` | NULL 不计，除以非 NULL 行数 | DOUBLE | 平均值（整数结果转 DOUBLE） |
| `MIN(column)` | **P0** | `MIN(column)` | NULL 不计 | 与列类型一致 | 最小值 |
| `MAX(column)` | **P0** | `MAX(column)` | NULL 不计 | 与列类型一致 | 最大值 |
| `STRING_AGG(column, separator)` | **P1** | `STRING_AGG(column, ',')` | NULL 不计，分隔符不为 NULL 则不出现 | STRING | 字符串聚合/拼接，用分隔符连接 |
| `GROUP_CONCAT(column [ORDER BY ...] [SEPARATOR ...])` | **P1** | `GROUP_CONCAT(name SEPARATOR ',')` | NULL 不计 | STRING | MySQL 风格字符串聚合（与 STRING_AGG 等价） |
| `STDDEV(column)` | **P2** | `STDDEV(column)` | NULL 不计 | DOUBLE | 样本标准差 |
| `VARIANCE(column)` | **P2** | `VARIANCE(column)` | NULL 不计 | DOUBLE | 样本方差 |

**实现说明**：
- 聚合函数在 `GROUP BY` 中作为列表达式使用，或在无 `GROUP BY` 时对全表聚合
- 支持 `HAVING` 子句对聚合结果过滤
- 聚合函数嵌套不支持（如 `MAX(COUNT(id))` 非法）

---

### 2. 字符串函数（String Functions）

| 函数 | 优先级 | 语法 | NULL 处理 | 返回类型 | 说明 | 兼容性 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `CONCAT(str1, str2, ...)` | **P0** | `CONCAT('Hello', ' ', 'World')` | 任意参数为 NULL 则返回 NULL | STRING | 字符串拼接 | SQL-92 标准 |
| `LENGTH(str)` / `LEN(str)` | **P0** | `LENGTH('Hello')` → 5 | NULL 返回 NULL | INT | 字符串长度（字节数） | 两种别名均支持 |
| `SUBSTRING(str, pos, len)` / `SUBSTR(str, pos, len)` | **P0** | `SUBSTRING('Hello', 2, 3)` → 'ell' | NULL 返回 NULL | STRING | 子串提取，pos 从 1 开始，超出返回空 | 兼容 MySQL/PG |
| `UPPER(str)` / `UCASE(str)` | **P0** | `UPPER('Hello')` → 'HELLO' | NULL 返回 NULL | STRING | 转大写 | 两种别名均支持 |
| `LOWER(str)` / `LCASE(str)` | **P0** | `LOWER('Hello')` → 'hello' | NULL 返回 NULL | STRING | 转小写 | 两种别名均支持 |
| `TRIM(str)` / `LTRIM(str)` / `RTRIM(str)` | **P0** | `TRIM('  Hello  ')` → 'Hello' | NULL 返回 NULL | STRING | 去除空白：TRIM(两端)、LTRIM(左)、RTRIM(右) | 常见 SQL 扩展 |
| `REPLACE(str, from, to)` | **P0** | `REPLACE('Hello', 'l', 'L')` → 'HeLLo' | NULL 返回 NULL | STRING | 字符串替换，替换所有匹配 | MySQL/PG 标准 |
| `LEFT(str, n)` | **P0** | `LEFT('Hello', 3)` → 'Hel' | NULL 返回 NULL | STRING | 左截取 n 个字符 | MySQL 扩展 |
| `RIGHT(str, n)` | **P0** | `RIGHT('Hello', 2)` → 'lo' | NULL 返回 NULL | STRING | 右截取 n 个字符 | MySQL 扩展 |
| `CHARINDEX(substr, str [, start])` | **P1** | `CHARINDEX('ll', 'Hello')` → 3 | NULL 返回 NULL | INT | 查找子串位置（不存在返回 0），start 默认 1 | SQL Server 风格 |
| `INSTR(str, substr)` | **P1** | `INSTR('Hello', 'll')` → 3 | NULL 返回 NULL | INT | 查找子串位置，MySQL 风格（与 CHARINDEX 等价） | MySQL 风格 |
| `CHAR(num)` | **P1** | `CHAR(65)` → 'A' | NULL 返回 NULL | STRING | ASCII 码转字符 | MySQL 标准 |
| `ASCII(str)` | **P1** | `ASCII('A')` → 65 | NULL 返回 NULL | INT | 字符转 ASCII 码（返回首字符）| 常见扩展 |
| `REVERSE(str)` | **P2** | `REVERSE('Hello')` → 'olleH' | NULL 返回 NULL | STRING | 反转字符串 | MySQL/PG 扩展 |
| `LPAD(str, len, pad)` | **P2** | `LPAD('5', 3, '0')` → '005' | NULL 返回 NULL | STRING | 左填充至指定长度，pad 默认空格 | MySQL 扩展 |
| `RPAD(str, len, pad)` | **P2** | `RPAD('5', 3, '0')` → '500' | NULL 返回 NULL | STRING | 右填充至指定长度，pad 默认空格 | MySQL 扩展 |
| `FORMAT(value, format)` | **不支持** | - | - | - | 复杂格式化（应用层实现） | |
| `SOUNDEX(str)` / `DIFFERENCE(s1, s2)` | **不支持** | - | - | - | 音似匹配（冷门，性能低） | |

---

### 3. 数值函数（Numeric Functions）

| 函数 | 优先级 | 语法 | NULL 处理 | 返回类型 | 说明 | 兼容性 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `ABS(n)` | **P0** | `ABS(-5)` → 5 | NULL 返回 NULL | 与参数类型一致 | 绝对值 | SQL-92 标准 |
| `ROUND(n, decimals)` | **P0** | `ROUND(3.14159, 2)` → 3.14 | NULL 返回 NULL | DECIMAL | 四舍五入，decimals=0 时取整 | SQL-92 标准 |
| `CEILING(n)` / `CEIL(n)` | **P0** | `CEILING(3.2)` → 4 | NULL 返回 NULL | INT | 向上取整 | 两种别名均支持 |
| `FLOOR(n)` | **P0** | `FLOOR(3.8)` → 3 | NULL 返回 NULL | INT | 向下取整 | SQL-92 标准 |
| `MOD(n, m)` / `n % m` | **P0** | `MOD(10, 3)` → 1 / `10 % 3` → 1 | NULL 返回 NULL | 与参数类型一致 | 取模运算 | 两种形式均支持 |
| `POWER(base, exp)` / `POW(base, exp)` | **P1** | `POWER(2, 3)` → 8 | NULL 返回 NULL | DOUBLE | 幂运算 | 两种别名均支持 |
| `SQRT(n)` | **P1** | `SQRT(16)` → 4.0 | NULL 返回 NULL | DOUBLE | 平方根 | 常见扩展 |
| `RAND()` / `RANDOM()` | **P1** | `RAND()` → 0.123... | 不适用 | DOUBLE | 随机数 (0.0 ~ 1.0) | 两种别名均支持 |
| `SIGN(n)` | **P1** | `SIGN(-5)` → -1 | NULL 返回 NULL | INT | 符号：负数-1，零0，正数1 | 常见扩展 |
| `TRUNCATE(n, decimals)` | **P1** | `TRUNCATE(3.14159, 2)` → 3.14 | NULL 返回 NULL | DECIMAL | 截断（不四舍五入） | MySQL 扩展 |
| `PI()` | **P2** | `PI()` → 3.14159... | 不适用 | DOUBLE | 圆周率 | 常见扩展 |
| `EXP(n)` | **P2** | `EXP(1)` → 2.718... | NULL 返回 NULL | DOUBLE | 自然指数 e^n | 常见扩展 |
| `LOG(n)` | **P2** | `LOG(100)` → 4.605... | NULL 返回 NULL | DOUBLE | 自然对数 ln(n) | 常见扩展 |
| `LOG10(n)` | **P2** | `LOG10(100)` → 2 | NULL 返回 NULL | DOUBLE | 常用对数 log10(n) | 常见扩展 |
| `SIN(n)` / `COS(n)` / `TAN(n)` | **不支持** | - | - | - | 三角函数（冷门，性能低） | |
| `ASIN(n)` / `ACOS(n)` / `ATAN(n)` | **不支持** | - | - | - | 反三角函数 | |

---

### 4. 日期时间函数（DateTime Functions）

| 函数 | 优先级 | 语法 | NULL 处理 | 返回类型 | 说明 | 兼容性 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `NOW()` / `GETDATE()` / `CURRENT_TIMESTAMP` | **P0** | `NOW()` → '2024-12-15 10:30:45.123' | 不适用 | DATETIME | 当前系统时间 | 三种别名均支持 |
| `YEAR(date)` | **P0** | `YEAR('2024-12-15')` → 2024 | NULL 返回 NULL | INT | 提取年份 | 常见扩展 |
| `MONTH(date)` | **P0** | `MONTH('2024-12-15')` → 12 | NULL 返回 NULL | INT | 提取月份 (1~12) | 常见扩展 |
| `DAY(date)` / `DAYOFMONTH(date)` | **P0** | `DAY('2024-12-15')` → 15 | NULL 返回 NULL | INT | 提取日期 (1~31) | 两种别名均支持 |
| `HOUR(time)` | **P0** | `HOUR('10:30:45')` → 10 | NULL 返回 NULL | INT | 提取小时 (0~23) | 常见扩展 |
| `MINUTE(time)` | **P0** | `MINUTE('10:30:45')` → 30 | NULL 返回 NULL | INT | 提取分钟 (0~59) | 常见扩展 |
| `SECOND(time)` | **P0** | `SECOND('10:30:45')` → 45 | NULL 返回 NULL | INT | 提取秒数 (0~59) | 常见扩展 |
| `DATEDIFF(date1, date2)` | **P0** | `DATEDIFF('2024-12-15', '2024-12-10')` → 5 | NULL 返回 NULL | INT | 日期差（天数），date1 - date2 | MySQL 风格 |
| `DATEADD(interval, n, date)` | **P0** | `DATEADD(MONTH, 1, '2024-12-15')` → '2025-01-15' | NULL 返回 NULL | DATETIME | 日期加减：interval 支持 YEAR/MONTH/DAY/HOUR/MINUTE/SECOND 等 | SQL Server 风格 |
| `DATE_ADD(date, INTERVAL n unit)` | **P0** | `DATE_ADD('2024-12-15', INTERVAL 1 MONTH)` → '2025-01-15' | NULL 返回 NULL | DATETIME | MySQL 风格日期加减（与 DATEADD 等价） | MySQL 风格 |
| `DATEPART(part, date)` | **P1** | `DATEPART(YEAR, '2024-12-15')` → 2024 | NULL 返回 NULL | INT | 提取日期部分（通用）：part 支持 YEAR/MONTH/DAY/HOUR/QUARTER/WEEK 等 | SQL Server 风格 |
| `DATE_FORMAT(date, format)` | **P1** | `DATE_FORMAT('2024-12-15', '%Y-%m-%d')` → '2024-12-15' | NULL 返回 NULL | STRING | 格式化输出，format 使用 MySQL 风格模式 | MySQL 风格 |
| `WEEKDAY(date)` | **P1** | `WEEKDAY('2024-12-15')` → 6 (周日=0) | NULL 返回 NULL | INT | 星期几，0=周日，6=周六 | MySQL 风格 |
| `DAYOFWEEK(date)` | **P1** | `DAYOFWEEK('2024-12-15')` → 1 (周日=1) | NULL 返回 NULL | INT | 星期几，1=周日，7=周六，SQL Server 风格 | SQL Server 风格 |
| `QUARTER(date)` | **P1** | `QUARTER('2024-12-15')` → 4 | NULL 返回 NULL | INT | 提取季度 (1~4) | 常见扩展 |
| `WEEK(date)` | **P1** | `WEEK('2024-12-15')` → 51 | NULL 返回 NULL | INT | 提取周数 (0~53) | 常见扩展 |
| `LAST_DAY(date)` | **P2** | `LAST_DAY('2024-12-15')` → '2024-12-31' | NULL 返回 NULL | DATE | 月末日期 | MySQL 扩展 |
| `TIMESTAMPDIFF(unit, date1, date2)` | **P2** | `TIMESTAMPDIFF(DAY, '2024-12-10', '2024-12-15')` → 5 | NULL 返回 NULL | LONG | 时间戳差（按单位）：unit 支持 SECOND/MINUTE/HOUR/DAY/MONTH/YEAR | MySQL 扩展 |
| `TIMESTAMPADD(unit, n, date)` | **P2** | `TIMESTAMPADD(DAY, 5, '2024-12-10')` → '2024-12-15' | NULL 返回 NULL | DATETIME | 时间戳加减 | MySQL 扩展 |

---

### 5. 类型转换与 NULL 处理（Type Conversion & NULL Functions）

| 函数 | 优先级 | 语法 | NULL 处理 | 返回类型 | 说明 | 兼容性 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `CAST(value AS type)` | **P0** | `CAST('123' AS INT)` → 123 | NULL 返回 NULL | 指定的 type | 标准类型转换，支持所有 NovaDb 类型 | SQL-92 标准 |
| `CONVERT(type, value)` | **P0** | `CONVERT(INT, '123')` → 123 | NULL 返回 NULL | 指定的 type | SQL Server 风格转换（与 CAST 等价） | SQL Server 风格 |
| `COALESCE(v1, v2, ...)` | **P0** | `COALESCE(NULL, NULL, 'default')` → 'default' | 返回第一个非 NULL 值 | 与第一个非 NULL 值类型一致 | 返回第一个非 NULL 值，全为 NULL 返回 NULL | SQL-92 标准 |
| `ISNULL(value, default)` | **P0** | `ISNULL(NULL, 'N/A')` → 'N/A' | 如果 value 为 NULL 返回 default，否则返回 value | 与 value 或 default 类型一致 | NULL 替换（SQL Server 风格） | SQL Server 风格 |
| `IFNULL(value, default)` | **P0** | `IFNULL(NULL, 'N/A')` → 'N/A' | 如果 value 为 NULL 返回 default，否则返回 value | 与 value 或 default 类型一致 | NULL 替换（MySQL 风格，与 ISNULL 等价） | MySQL 风格 |
| `NULLIF(v1, v2)` | **P1** | `NULLIF(5, 5)` → NULL；`NULLIF(5, 10)` → 5 | 相等时返回 NULL，否则返回 v1 | 与 v1 类型一致 | 相等则转 NULL 的逆向函数 | SQL-92 标准 |

---

### 6. 条件函数（Conditional Functions）

| 函数 | 优先级 | 语法 | NULL 处理 | 返回类型 | 说明 | 兼容性 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `CASE WHEN ... THEN ... ELSE ... END` | **P0** | `CASE WHEN age > 18 THEN 'Adult' ELSE 'Minor' END` | 分支条件为 NULL 当作 FALSE 处理 | WHEN/THEN 返回值类型统一 | 标准条件表达式，支持多个 WHEN，必须有 ELSE（或隐式 NULL） | SQL-92 标准 |
| `IF(condition, true_val, false_val)` | **P0** | `IF(age > 18, 'Adult', 'Minor')` | condition 为 NULL 当作 FALSE，返回 false_val | 与 true_val/false_val 类型统一 | MySQL 风格条件（三元运算符） | MySQL 风格 |
| `IIF(condition, true_val, false_val)` | **P1** | `IIF(age > 18, 'Adult', 'Minor')` | condition 为 NULL 当作 FALSE，返回 false_val | 与 true_val/false_val 类型统一 | SQL Server 风格条件（与 IF 等价） | SQL Server 风格 |

---

### 7. 系统/元数据函数（System & Metadata Functions）

| 函数 | 优先级 | 语法 | 返回值 | 说明 | 备注 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `DATABASE()` / `CURRENT_DATABASE()` | **P1** | `DATABASE()` | STRING | 当前数据库名 | 两种别名均支持 |
| `VERSION()` | **P1** | `VERSION()` | STRING | 数据库版本（如 `NovaDb 1.0.2024.1201`） | 返回 NovaDb 版本号 |
| `ROW_COUNT()` / `@@ROWCOUNT` | **P1** | `ROW_COUNT()` | INT | 上一条 DML (INSERT/UPDATE/DELETE) 影响的行数 | 两种形式均支持 |
| `LAST_INSERT_ID()` / `@@IDENTITY` | **P1** | `LAST_INSERT_ID()` | LONG | 最后插入的自增主键 ID（INSERT 后） | 两种别名均支持，仅在 INSERT IDENTITY 列后可用 |
| `USER()` / `CURRENT_USER()` | **P2** | `USER()` | STRING | 当前连接用户名（v1 无权限系统，返回 `'nova'`） | 两种别名均支持 |
| `CONNECTION_ID()` | **P2** | `CONNECTION_ID()` | LONG | 当前连接的唯一 ID | 服务器模式用于区分不同连接 |

---

### 8. 地理位置函数（GeoPoint Functions） - NovaDb 扩展

| 函数 | 优先级 | 语法 | NULL 处理 | 返回类型 | 说明 | 备注 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `DISTANCE(point1, point2)` | **P1** | `DISTANCE(geopoint1, geopoint2)` | 任意参数为 NULL 返回 NULL | DOUBLE | 两点间地理距离（米），使用 Haversine 大圆距离公式 | 精度: 误差 < 0.5% |
| `WITHIN_RADIUS(point, center, radius)` | **P1** | `WITHIN_RADIUS(user_location, '(39.9, 116.4)', 1000)` | 任意参数为 NULL 返回 NULL | BOOLEAN | 判断 point 是否在以 center 为中心、半径为 radius(米) 的圆形范围内 | 用于范围查询 WHERE 条件 |
| `DISTANCE_KM(point1, point2)` | **P1** | `DISTANCE_KM(geopoint1, geopoint2)` | 任意参数为 NULL 返回 NULL | DOUBLE | 两点间距离（公里）| 单位为公里，方便地理应用 |
| `WITHIN_POLYGON(point, polygon_wkt)` | **P2** | `WITHIN_POLYGON(location, 'POLYGON((lat1 lon1, lat2 lon2, ...))' )` | 任意参数为 NULL 返回 NULL | BOOLEAN | 判断点是否在多边形区域内（WKT 格式） | 用于复杂区域判断，规划中 |

---

### 9. 向量函数（Vector Functions） - NovaDb 扩展

| 函数 | 优先级 | 语法 | NULL 处理 | 返回类型 | 说明 | 备注 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `COSINE_SIMILARITY(vec1, vec2)` | **P1** | `COSINE_SIMILARITY(embedding1, embedding2)` | 任意参数为 NULL 返回 NULL | DOUBLE | 向量余弦相似度 [-1, 1]，值越大越相似 | 常用于语义搜索 |
| `EUCLIDEAN_DISTANCE(vec1, vec2)` | **P1** | `EUCLIDEAN_DISTANCE(vec1, vec2)` | 任意参数为 NULL 返回 NULL | DOUBLE | 向量欧氏距离（L2 范数），值越小越相似 | 用于向量相似搜索 |
| `DOT_PRODUCT(vec1, vec2)` | **P2** | `DOT_PRODUCT(vec1, vec2)` | 任意参数为 NULL 返回 NULL | DOUBLE | 向量点积（内积），结合归一化向量用于相似度计算 | 性能优于余弦相似度 |
| `VECTOR_NEAREST(query_vec, table, top_k, metric)` | **P2** | `SELECT * FROM embeddings ORDER BY VECTOR_NEAREST(embedding, query_vec, 10, 'cosine')` | query_vec 为 NULL 返回 NULL | - | Top-K 最相似向量查询（规划），metric 支持 'cosine'/'euclidean'/'dot' | 规划在 ORDER BY 中支持 |

---

### 10. 加密/哈希函数（Hash & Crypto Functions）

| 函数 | 优先级 | 语法 | NULL 处理 | 返回类型 | 说明 | 备注 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `MD5(str)` | **P1** | `MD5('password')` → '5f4dcc3b5aa765d61d8327deb882cf99' | NULL 返回 NULL | STRING | MD5 哈希（32 字符十六进制） | 仅用于校验和，不可逆，不用于密码 |
| `SHA1(str)` | **P1** | `SHA1('password')` → 'e38ad214943daad1d64c102faec29de4afe9fd3d' | NULL 返回 NULL | STRING | SHA1 哈希（40 字符十六进制） | 已不安全，不推荐用于密码 |
| `SHA2(str, bits)` | **P1** | `SHA2('password', 256)` → '5e884898da28047...' | NULL 返回 NULL | STRING | SHA2 哈希，bits 支持 256/384/512 | 推荐用于密码哈希 |
| `ENCRYPT(str, key)` / `AES_ENCRYPT(str, key)` | **不支持** | - | - | - | 加密（应用层实现） | 不包含在 v1 范围内 |

---

### 11. 正则表达式函数（Regex Functions）

| 函数 | 优先级 | 说明 | 备注 |
| :--- | :--- | :--- | :--- |
| `REGEXP(str, pattern)` / `RLIKE(str, pattern)` | **不支持** | 正则匹配（性能开销大，v1 不实现） | |
| `REGEXP_REPLACE(str, pattern, replacement)` | **不支持** | 正则替换 | |
| `REGEXP_SUBSTR(str, pattern)` | **不支持** | 正则子串提取 | |

---

### 12. 窗口函数（Window Functions）

| 函数类型 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `ROW_NUMBER()`、`RANK()`、`DENSE_RANK()` | **不支持** | v1 不实现窗口函数，规划 v1.1+ |
| `LAG()`、`LEAD()` | **不支持** | |
| `FIRST_VALUE()`、`LAST_VALUE()` | **不支持** | |

---

### 13. JSON 函数（JSON Functions）

| 函数 | 优先级 | 说明 |
| :--- | :--- | :--- |
| `JSON_EXTRACT(json, path)` | **P2** | JSON 提取，v1 不实现，规划 v1.1+ |
| `JSON_SET(json, path, value)` | **P2** | |
| `JSON_ARRAY()` / `JSON_OBJECT()` | **P2** | |

---

## 实现与测试说明

### 函数实现检查清单

| 项目 | 要求 |
| :--- | :--- |
| **语法完整性** | ✅ 支持完整的函数签名与参数变体（如 SUBSTRING(str, pos, len) 与 SUBSTRING(str FROM pos FOR len) 等）|
| **NULL 传播** | ✅ 准确实现 NULL 处理逻辑（多数标量函数为 NULL 传播，聚合函数为 NULL 不计） |
| **类型强制转换** | ✅ 参数自动转换（如 `ROUND('3.14', 2)` 字符串自动转 DECIMAL）|
| **错误处理** | ✅ 参数范围错误返回 NULL（如 `SUBSTRING(str, -1, 10)` 返回 NULL）而非异常 |
| **性能基准** | ✅ 百万行数据下函数执行时间 < 1ms（除地理/向量复杂计算外）|
| **测试覆盖** | ✅ 正常值、NULL、边界值、类型不匹配场景均需测试用例 |

### 优先级实施建议

1. **v1.0 必须完成**：所有 P0 函数（共 40+ 个）
2. **v1.0 建议完成**：P1 函数中高频函数（字符串、日期、类型转换、地理等，共 60+ 个）
3. **v1.1+ 规划**：P2 及扩展函数（窗口函数、JSON、正则等）

### 兼容性测试场景

- **场景 1**：多数据库迁移 - 确保常见 MySQL/PG/SQL Server 函数可无缝迁移
- **场景 2**：ORM 集成 - 支持 XCode/EF Core 常用函数投影
- **场景 3**：性能压力 - 百万行聚合/字符串操作不超时

---

**文档版本**：v1.0  
**最后更新**：2026年2月23日  
**维护者**：NewLife 开发团队
