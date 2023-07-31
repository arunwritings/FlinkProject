package org.flinkjob.connectors;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

public class JDBCExample {
    public static void main(String[] args) throws Exception {
        String driverName = "com.mysql.cj.jdbc.Driver";
        String sourceDB = "arundb";
        String sinkDB = "arundb";
        String dbURL = "jdbc:mysql://localhost:3306/";
        String dbUser = "root";
        String dbPassword = "root";
        String selectQuery = "select * from flink";

        BasicTypeInfo basicTypeInfo;
        TypeInformation<?>[] types = new TypeInformation<?>[]
                {BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
                        , BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types);
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        JDBCInputFormat.JDBCInputFormatBuilder input = JDBCInputFormat.buildJDBCInputFormat().setDrivername(driverName).
                        setDBUrl(dbURL + sourceDB).
                        setUsername(dbUser).setPassword(dbPassword).setRowTypeInfo(rowTypeInfo).
                        setQuery(selectQuery);

        DataSet<Row> source = environment.createInput(input.finish());
        source.print();

        String insertSQL = "INSERT INTO flink_target (id,name,email,phone,country,active_flag) values (?, ?,?,?,?,?)";
        JDBCOutputFormat.JDBCOutputFormatBuilder output = JDBCOutputFormat.buildJDBCOutputFormat().setDBUrl(dbURL + sinkDB).setDrivername(driverName).
                        setQuery(insertSQL).setUsername(dbUser).setPassword(dbPassword).
                        setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
                                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});

        source.output(output.finish());

        environment.execute();
    }

}
