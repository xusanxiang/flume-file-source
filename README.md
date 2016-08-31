FileSource 基于Flume 1.6版本，

能够监控单个文件的内容更改并读取文件改动内容

1. 根据文件最后修改时间判断是否已经修改
2. 根据正则表达式作为行判断，将表达式之后的内容合为一个Event
3. 文件被log4j或者其他原因Roll之后，能够自动换句柄，重新读取文件
4. 根据线程池的启动时间以及Buffer大小做流控


本项目最初的模版是：
https://github.com/cwtree/flume-filemonitor-source
在此项目基础上进行了修改

如有疑问，请联系我：

QQ: 877964878

Email: sdzcxyx@163.com
