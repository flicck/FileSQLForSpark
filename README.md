# FileSQLForSpark
FileSQLForSpark解决了Spark输入文件不够灵活的痛点，其可以让程序员使用一种类似SQL的语句让spark同时读取HDFS目录和本地目录上的不同数据源文件。它
无疑加强了Spark文件输入的灵活性，也大大降低了Spark内存调优的时间和资源成本。

### 1.FileSQLForSpark 的特点：
#### 1.1 支持使用union关键字多数据源输入(orc avro text json parquet csv等)
#### 1.2 支持传入不同数据源的struct(已在上层封装,更好用了)，按字段名称合并成一个有结构dataframe
#### 1.3 支持将不同数据源的合并成一个无结构dataframe
#### 1.4 支持对不同数据源目录进行递归
#### 1.5 支持time关键字对输入文件的时间进行限制（创建时间，如果有修改，为最后一次修改时间）
#### 1.6 支持limit关键字对输入文件数量进行限制
#### 1.7 支持正则关键字rlike正向匹配和ulike反向匹配
#### 1.8 支持通过关键字fileSize按不同数据源单个文件大小筛选数据源文件
#### 1.9 支持通过关键字totalSize按不同数据源总文件大小筛选数据源文件
#### 1.10 运行时会输出使用的sql语句，输入文件，以及所有输入数据的字节数，方便Spark调优

### 2.FileSQLForSpark SQL语法说明
>来一个典型的SQL例子
#### "select text from /users/wanghan/input1 where fileSize > 100m and < 300m and time >= 2019-2-1 rlike 'hello' limit 4 union select json 
#### from /users/wanghan/input2 where totalSize < 2g limit 3" （符号与单词之间均需要空格）
运行时,需在args中传入上述参数。如上代表从/users/wanghan/input1目录下读取text格式文件，从/users/wanghan/input2目录下读取json格式文件
同时对text数据源输入的单个文件大小、文件名、文件数量、文件时间进行了限制，对json数据源的文件数量和输入文件的总大小进行了限制。  
 > FileSQLForSpark提供了getDataFrameUnionByName(),使用该方法,传入你继承的PreStructWithDelimits或PreStructWithOutDelimits
 > 类以描述结构信息，便可以将不同数据源的数据结构化的整合成一个DataFrame。不同数据源的缺失字段会补齐为null  
 > FileSQLForSpark也提供了getDataFrameSimpleUnion，该方法无需传入结构，返回一个只有一个value字段的DataFrame
 > 这时，json等含有结构的数据源读取的过程中，字段之间会加入\001分隔符，缺失字段同样会用null进行补齐

