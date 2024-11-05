说明文档

一、使用前准备
环境：
Python 3.6.13（其他Python3版本亦可）
Apache Maven 3.8.3
Java 1.8.0

使用pip安装依赖库：
psycopg2, sqlparse, jpype1等。
其中jpype1就是jpype库。

进入有main.py文件的目录，先执行
mvn dependency:build-classpath -Dmdep.outputFile=classpath.txt
然后打开classpath.txt，将calcite-core-1.26.0.jar的路径改为当前目录/libs/calcite-core-1.26.0-SNAPSHOT.jar



二、基本使用
数据集为dataset/valid_sqls.log，每行一条待优化的SQL
然后执行main.py进行改写即可，默认为mcts方法，命令如下：
python main.py


三、参数：
1、数据库相关
--host 数据库服务器IP
--dbname 数据库名
--port 端口号
--user 数据库用户名
--password 密码
--driver 用于calcite的driver

2、数据集
--sqls 待改写的SQL数据的文件名，每行一条SQL
--starter 从第几条SQL开始改写
--howmany 改写多少条，-1表示改写全部

3、算法
--policy 算法名，备选：[default, mcts]
default为calcite默认改写，每次匹配第一条可使用的规则。
mcts为蒙特卡洛树搜索，即learned rewrite采用的方案。

4、算法参数
--mctssteps 蒙特卡洛树搜索的步数
--mctsgamma 蒙特卡洛树搜索的探索系数，越大越倾向于探索访问次数少的节点

5、其他
--saveresult 文件名，用于保存修改路径信息，默认不保存。
--verbose 控制打印信息级别，默认为1。若为0则rewrite函数不打印任何信息（仅main函数打印少量信息），若为1则打印更复杂的信息。
--records 文件名，用于保存改写结果，对所有能够降低cost的改写，记录改写前后的SQL与cost。



四、特定情况不做改写
1、字符串补齐
特定情况下，Calcite会对一些字符串进行ljust(25)操作，即右侧补齐空格至长度为25（该数值在rules.py的padding_length指定，若新环境测试发现不是这个数值，则在rules.py内修改即可）。
为避免错误的发生，目前的代码记录了所有字符串的ljust(25)（在check_string函数内），在生成SQL、进行cost estimation时均先将ljust后的字符串还原（在RA2SQL函数内）。
但如果存在两个不同的字符串，它们的ljust(25)相同，则仍然会出错。因此我们在改写前会做检查，如果SQL出现此情况，则不做改写。

2、异常
如果将SQL转为Calcite执行计划再转为SQL放入PG进行EXPLAIN的过程中发生异常，则不做改写（try-except形式）。
例如zone单独出现且不在引号内，sqlparse会认为它是保留字（尽管它可能是表名/列名），而Calcite没有这个保留字，会造成异常。

