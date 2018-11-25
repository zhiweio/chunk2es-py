# chunk2es-py

chunk2es-py 是一个同步文本数据到 elasticsearch 的脚本工具， 它依赖于 `elasticsearch-py`

## Install
```bash
下载git安装包到本地
$ git clone https://github.com/Zhiwei1996/chunk2es-py.git
$ cd chunk2es-py
建立python虚拟环境，并安装依赖
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

## How to use

**配置**

编辑`sync.conf`文件作同步任务的相关配置
- `hosts`是elasticsearch的链接参数，完全按照`elasticsearch-py`的`Elasticsearch`类的语法，[官方文档](https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch)
- `_index`指定要同步数据的elasticsearch索引(index)名称
- `_type`指定elasticsearch的类型(type)名称
- `delimiter`指定你要同步的文本中每一行数据字段间的分隔符
- `headline`指定数据的各个字段名
- `ingore`指定忽略同步的字段，必须是`headline`里的
```json
Example:

{
    "hosts": [
        {"host": "localhost", "port": 9200}
    ],
    "_index": "bank",
    "_type": "account",
    "_id": "ID",
    "delimiter": "\t",
    "headline": ["ID", "USER_ID", "EMAIL", "USERNAME", "PASSWORD"],
    "ingore": ["ID"]
}
```

**参数说明**

```bash
$ python chunk2es.py -h
usage: chunk2es.py [-h] [-f FILE] [-c CONFIG]

tool for export data from file to elasticsearch

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --input-file FILE
                        read ip addresses from a txt file
  -c CONFIG, --config CONFIG
                        specify config file about ES
```
`-f`指定要同步的文本，`-c`指定配置文件
```bash
$ python chunk2es.py -f test.txt -c sync.conf
```

**日志**

运行过程中生成 `stderr.log` 日志文件，数据中存在字段缺失和多余的行跳过并记录到 log 中

**后台启动**

通过 Linux 的 `bg` 命令来实现后台进程，`Ctrl+Z` 是休眠进程命令
```bash
$ python chunk2es.py -f test.txt -c sync.conf 2> runError.log
Ctrl+Z
$ bg
```
通过 `supervisor` 管理

请看一下 `supervisor` 文档，如何配置管理任务

**效率**

没做过详细测试，每条数据不大的话，可能接近5000/qps（本地）

elasticsearch 9200的端口可能上限就是5000/qps了

具体效率要看实际场景，带宽，机器性能等等

同时跑多个脚本同步数据并不会有速度提高，而且会部分数据的索引失败

> **官方建议**
>
> **How Big Is Too Big?edit**
>
> The entire bulk request needs to be loaded into memory by the node that receives our request, so the bigger the request, the less memory available for other requests. There is an optimal size of bulk request. Above that size, performance no longer improves and may even drop off. The optimal size, however, is not a fixed number. It depends entirely on your hardware, your document size and complexity, and your indexing and search load.
>
> Fortunately, it is easy to find this sweet spot: Try indexing typical documents in batches of increasing size. When performance starts to drop off, your batch size is too big. A good place to start is with batches of 1,000 to 5,000 documents or, if your documents are very large, with even smaller batches.
>
> It is often useful to keep an eye on the physical size of your bulk requests. One thousand 1KB documents is very different from one thousand 1MB documents. A good bulk size to start playing with is around 5-15MB in size.
## Notice
需要同步的文本数据一定要是相同格式的，比如下面这样全部以 `tab` 作为字段分隔符的
```json
207628234	387477787	neso0013@net.hr	387477787
207628235	387477790	sasha_sniker@yahoo.com	387477790
207628236	387477791	mendozamaryfer@yahoo.com	387477791
207628237	387477792	pexon_g@yahoo.com	387477792
207628238	387477796	aquacrafters@msn.com	mrdannybuchanan
207628239	387477799	server_msn_security@hotmail.es	streetaliancefamily10
207628240	387477801	JUNIOR2356@YAHOO.COM	387477801
207628241	387477802	ellamarie916@yahoo.com	smella17
207628242	387477805	support@singlesdatingpedia.com	yvonnerice
207628243	387477808	meganmclean@hotmail.co.uk	387477808
```
暂时不支持原生 csv 文本，可以手动去掉 csv 文本的头，然后指定逗号为分隔符来进行数据同步

## How work
工作原理图
![workfolw](https://github.com/Zhiwei1996/chunk2es-py/raw/master/test/chunk2es-py.png)
就这么简单 :)

--------------
有bug请联系我，noparking188@gmail.com
