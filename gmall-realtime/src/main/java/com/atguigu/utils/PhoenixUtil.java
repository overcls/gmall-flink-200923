package com.atguigu.utils;

import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Date:2021/3/20
 * Description:
 */
public class PhoenixUtil {

    //定义连接
    private static Connection connection;

    //定义connection的get方法，用来获取连接
    private static Connection getConnection() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            return DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取Phoenix连接失败");
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> clz, Boolean underScoreToCamel) {

        //若连接为空，可获取连接
        if (connection == null) {
            connection = getConnection();
        }

        //定义预编译环境
        PreparedStatement preparedStatement = null;

        //定义SQL执行结果集
        ResultSet resultSet = null;

        //定义返回数据集
        ArrayList<T> resultList = new ArrayList<>();

        try {
            //预编译SQL
            preparedStatement = connection.prepareStatement(sql);

            //执行SQL，获取结果集
            resultSet = preparedStatement.executeQuery();

            //获取元数据相关信息
            ResultSetMetaData metaData = resultSet.getMetaData();
            //获取表中字段数目
            int columnCount = metaData.getColumnCount();

            //遍历结果集
            while (resultSet.next()) {

                //创建T类型的对象t
                T t = clz.newInstance();

                //遍历表中各个字段
                for (int i = 1; i < columnCount + 1; i++) {
                    //以索引获取表字段名
                    String columnName = metaData.getColumnName(i);

                    //更改字段的命名方式
                    if (underScoreToCamel) {
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //以字段名获取字段值
                    String values = resultSet.getString(columnName);

                    //赋值给对象t
                    BeanUtils.setProperty(t, columnName, values);
                }
                //将泛型对象加入集合
                resultList.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("查询维度" + sql + "信息失败");
        } finally {
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
        }
        return resultList;
    }
}
