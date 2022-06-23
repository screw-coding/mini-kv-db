# minikvdb
理解 bitcask 存储模型

需要说明的是，minikvdb 没有实现  bitcask 模型的多个数据文件的机制，为了简单，只使用了一个数据文件进行读写。

当然，你可以阅读 bitcask 模型的论文原文：[https://riak.com/assets/bitcask-intro.pdf](https://riak.com/assets/bitcask-intro.pdf)



