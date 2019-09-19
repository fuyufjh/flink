mvn install -pl "!flink-runtime-web" -T8 -DskipTests -Dcheckstyle.skip -Dscalastyle.skip -Drat.skip=true

# archive and copy to my downloads folder
pushd flink-dist/target/flink-1.9-tpcds-master-bin/
tar --exclude=flink-1.9-tpcds-master/opt/ -zcvf flink-1.9-tpcds-master.tar.gz flink-1.9-tpcds-master/
popd
mv flink-dist/target/flink-1.9-tpcds-master-bin/flink-1.9-tpcds-master.tar.gz ~/Downloads/
