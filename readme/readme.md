项目在优化内存分配时，使用了lwjgl库，需要配合本地jar包使用。
将readme目录下的 lwjgl-3.2.3-natives-linux.jar 和 lwjgl-3.2.3-natives-macos.jar 文件拷贝到flink-dist/target/flink-1.9-tpcds-master-bin/flink-1.9-tpcds-master/lib目录下
（和parquet相关的jar包同理）
一起打包，上传平台即可运行。
