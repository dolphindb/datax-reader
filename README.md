# 基于 DataX 的 DolphinDB 数据读取工具

## 1. DataX 离线数据同步

DataX 是在阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、DRDS 等各种异构数据源之间高效的数据同步功能，详情可查看 [DataX 已支持的数据源](https://github.com/alibaba/DataX/blob/master/README.md#support-data-channels)。

DataX 是可扩展的数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的 Reader 插件，以及向目标端写入数据的 Writer 插件。理论上 DataX 框架可以支持任意数据源类型的数据同步工作。每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。

### DataX 插件：dolphindbreader

基于 DataX 的扩展功能，dolphindbreader 插件实现了从 DolphinDB 读出数据。使用 DataX 官方现有的 writer 插件结合 dolphindbreader 插件，即可满足从 DolphinDB 向不同数据源同步数据。

dolphindbreader 底层依赖 DolphinDB Java API，采用批量读出的方式将分布式数据库的数据读出。

注意：如果一次性读出的 DolphinDB 源表过大，会造成插件 OOM 报错，建议使用读出数据量在 200 万以下的表。

## 2. 使用方法

详细信息请参阅 [DataX 指南](https://github.com/alibaba/DataX/blob/master/userGuid.md)，以下仅列出必要步骤。**注意，dataX 的启动脚本基于 python2 开发，所以需要使用 python2 来执行 **[**datax.py**](http://datax.py/)**。**

### 2.1 下载部署 DataX

[点击下载 DataX](https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202210/datax.tar.gz)。

### 2.2 部署 DataX-DolphinDBReader 插件

将源码的 `./dolphindbreader` 目录及下所有内容拷贝到 `datax/plugin/reader` 目录下，即可使用。

### 2.3 执行 DataX 任务

进入 `datax/bin` 目录下，用 python2 执行 `datax.py` 脚本，并指定配置文件地址，示例如下：

```
cd /root/datax/bin/
python2 datax.py /root/datax/myconf/BASECODE.json
```

### 2.4 导出实例

使用 DataX 的绝大部分工作都是通过配置来完成，包括双边的数据库连接信息和需要同步的数据表结构信息等。

#### 示例：全量导入

下面以从 DolphinDB 向 Oracle 导入一张表 BASECODE 进行示例。

首先在导入前，需要在 Oracle 中预先创建好目标数据库和表；然后使用 dolphindbreader 从 DolphinDB 读取 BASECODE 源表全量数据；再使用 oraclewriter 将读取到的 BASECODE 数据写入 Oracle 对应的目标表中。

编写配置文件 BASECODE.json，并存放到指定目录，比如 `/root/datax/myconf `目录下，配置文件说明请参考附录一。

配置完成后，在 `datax/bin` 目录下执行如下脚本即可启动数据同步任务。

```
cd /root/datax/bin/
python2 datax.py /root/datax/myconf/BASECODE.json
```

## 3.附录

### 附录一：配置文件示例

BASECODE.json

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 1
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "dolphindbreader",
                    "parameter": {
                        "userId": "admin",
                        "pwd": "123456",
                        "host": "47.99.175.197",
                        "port": 8848,
						"dbPath": "",
						"tableName": "stream5",
						"where": "",
						"table": [{
								"name": "bool"
							},
							{
								"name": "short"
							}
						]
                    }
                },
               "writer": {
                    "name": "oraclewriter",
                    "parameter": {
                        "username": "system",
                        "password": "DolphinDB123",
                        "column":["*"],
                        "connection":[
                            {
                            "jdbcUrl":"jdbc:oracle:thin:@192.168.0.9:1521/orcl",
                            "table":[
                                "wide_table"
                                ]
                            }
                        ]
                    }
                }
            }
        ]
    }
}
```

#### 配置文件参数说明

（仅介绍 dolphindbreader 的参数，writer 参数根据写入目标数据库的不同而不同，详情参阅：[datax指南](https://github.com/alibaba/DataX/blob/master/userGuid.md) ）

- host
  - 描述：Server Host。
  - 必选：是。
  - 默认值：无。
- port
  - 描述：Server Port。
  - 必选：是。
  - 默认值：无。
- userId
  - 描述：DolphinDB 用户名。导出分布式库时，必须要有权限的用户才能操作，否则会返回。
  - 必选：是。
  - 默认值：无。
- pwd
  - 描述：DolphinDB 用户密码。
  - 必选：是。
  - 默认值：无。
- dbPath
  - 描述：需要读出的目标分布式库名称，比如 `dfs://MYDB`。
  - 必选：是。
  - 默认值：无
- tableName
  - 描述: 读出数据表名称。
  - 必选: 是。
  - 默认值: 无。
- where
  - 描述: 可以通过指定 where 来设置条件，比如 "id >10 and name = `dolphindb"。
  - 必选: 是。
  - 默认值: 无。
-  table
   - 描述：读出表的字段集合。内部结构为：`{"name": "columnName"}`。请注意此处列定义的顺序，需要与原表提取的列顺序完全一致。
   - 必选: 是。
   - 默认值: 无。
- name：
  - 描述：字段名称。
  - 必选: 是。
  - 默认值: 无。

### 附录二：数据对照表（其他数据类型暂不支持）

| DolphinDB类型  | 配置值             | DataX类型 |
| ------------ | --------------- | ------- |
| DOUBLE       | DT_DOUBLE       | DOUBLE  |
| FLOAT        | DT_FLOAT        | DOUBLE  |
| BOOL         | DT_BOOL         | BOOLEAN |
| DATE         | DT_DATE         | DATE    |
| DATETIME     | DT_DATETIME     | DATE    |
| TIME         | DT_TIME         | STRING  |
| TIMESTAMP    | DT_TIMESTAMP    | DATE    |
| NANOTIME     | DT_NANOTIME     | STRING  |
| NANOTIMETAMP | DT_NANOTIMETAMP | DATE    |
| MONTH        | DT_MONTH        | DATE    |
| BYTE         | DT_BYTE         | LONG    |
| LONG         | DT_LONG         | LONG    |
| SHORT        | DT_SHORT        | LONG    |
| INT          | DT_INT          | LONG    |
| UUID         | DT_UUID         | STRING  |
| STRING       | DT_STRING       | STRING  |
| BLOB         | DT_BLOB         | STRING  |
| SYMBOL       | DT_SYMBOL       | STRING  |
| COMPLEX      | DT_COMPLEX      | STRING  |
| DATEHOUR     | DT_DATEHOUR     | DATE    |
| DURATION     | DT_DURATION     | LONG    |
| INT128       | DT_INT128       | STRING  |
| IPADDR       | DT_IPADDR       | STRING  |
| MINUTE       | DT_MINUTE       | STRING  |
| MONTH        | DT_MONTH        | STRING  |
| POINT        | DT_POINT        | STRING  |
| SECOND       | DT_SECOND       | STRING  |
