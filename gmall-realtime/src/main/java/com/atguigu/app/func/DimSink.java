package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * Date:2021/3/19
 * Description:
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    //定义phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * jsonObject:{"database":"","table":"","type":"","data":{"id":"11"...},"sinkTable":"dim_xxx_xxx"}
     * SQL:upsert into schema.table (id,name,sex) values(..,..,..)
     * @param jsonObject
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

        PreparedStatement preparedStatement = null;

        //获取表名
        String table = jsonObject.getString("sinkTable");

        //获取字段名和字段值
        JSONObject data = jsonObject.getJSONObject("data");

        Set<String> keys = data.keySet();

        Collection<Object> values = data.values();

        try {
            //获取SQL语句
            String upsertSql = getUpsertSql(table,keys,values);
            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);
            //执行预编译
            preparedStatement.execute();
            //提交任务
            connection.commit();
            //如果数据为更新数据，则删除Redis缓存中的数据（因为该数据已经过时）
            if("update".equals(jsonObject.getString("type"))){
                String value = jsonObject.getJSONObject("data").getString("id");
                String key = table + ":" + value;
                DimUtil.deleteCache(key);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入维度数据到"+table+"失败");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    //SQL:upsert into schema.table (id,name,sex) values('..','..','..')
    private String getUpsertSql(String table, Set<String> keys, Collection<Object> values) {
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + table + "" +
                "(" + StringUtils.join(keys,",") + ")" +
                " values('" + StringUtils.join(values,"','") + "')";
    }
}
