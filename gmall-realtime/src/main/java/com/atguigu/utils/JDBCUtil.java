package com.atguigu.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Date:2021/3/19
 * Description:
 */
public class JDBCUtil {
    public static <T> List<T> queryList(String sql,Class<T> clz,Boolean underScoreToCamel){
        //定义集合存放结果
        ArrayList<T> result = new ArrayList<>();
        //定义连接
        Connection connection = null;
        //定义预编译
        PreparedStatement preparedStatement = null;
        //定义结果集
        ResultSet resultSet = null;
        try {
            //加载JDBC的连接驱动
            Class.forName("com.mysql.jdbc.Driver");
            //获取连接
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/gmall2021-realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456"
            );
            //预编译SQL
            preparedStatement = connection.prepareStatement(sql);
            //执行查询
            resultSet = preparedStatement.executeQuery();
            //获取表的元数据
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            //通过元数据获取到字段的数量
            int columnCount = metaData.getColumnCount();
            //遍历resultSet结果集
            while(resultSet.next()){
                //通过反射的方式拿到T对象
                T t = clz.newInstance();
                //遍历表中所有列
                for (int i = 1; i < columnCount + 1; i++) {
                    String columnName = metaData.getColumnName(i);
                    //判断是否需要转换字符串
                    if(underScoreToCamel){
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    //获取值信息
                    String value = resultSet.getString(i);
                    //给对象赋值
                    BeanUtils.setProperty(t,columnName,value);
                }
                //将对象放入集合
                result.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //返回结果
        return result;
    }
}
