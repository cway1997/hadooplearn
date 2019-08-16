package com.cway.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseUtils {

    private Configuration conf;
    private Connection conn;
    private HBaseAdmin admin;

    public HBaseUtils() {
        conf = HBaseConfiguration.create();
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放资源
     * @return
     */
    public int close() {
        int status = 0;
        try {
            conn.close();
            admin.close();
            status = 1;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return status;
    }

    /**
     * 获取表列表
     * @return
     */
    public String[] listTables() {
        ArrayList<String> tables = new ArrayList<>();
        try {
            HTableDescriptor[] hTableDescriptors = admin.listTables();
            for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
                tables.add(hTableDescriptor.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tables.toArray(new String[tables.size()]);
    }

    /**
     * 创建表
     * @param tbName
     * @param cls
     * @return
     */
    public int createTable(String tbName, String[] cls) {
        int status = 0;
        TableName htb = TableName.valueOf(tbName);
        try {
            if (admin.tableExists(htb)) {
                System.out.println("table is exists!");
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(htb);
                for (String cf : cls) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
                System.out.println("table is created success.");
            }
            status = 1;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return status;
    }

    /**
     * 删除表
     * @param tbName
     * @return
     */
    public int delTable(String tbName) {
        int status = 0;
        try {
            TableName htb = TableName.valueOf(tbName);
            if (admin.tableExists(htb)) {
                admin.disableTable(htb);
                admin.deleteTable(htb);
            }
            status = 1;
            System.out.println("drop table success.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }


    /**
     * 插入单条数据
      * @param tbName
     * @param rowKey
     * @param colFamily
     * @param col
     * @param value
     * @return
     */
    public int insertData(String tbName, String rowKey, String colFamily, String col, String value) {
        int status = 0;
        try {
            Table table = conn.getTable(TableName.valueOf(tbName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
            table.put(put);
            table.close();
            status = 1;
            System.out.println("one row insert success.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    /**
     * 批量插入
     * @param tbName
     * @param rows
     * @return
     */
    public int insertDataBatch(String tbName, List<String[]> rows) {
        int status = 0;
        try {
            Table table = conn.getTable(TableName.valueOf(tbName));
            List<Put> putList = new ArrayList<>();
            for (String[] row : rows) {
                Put put = new Put(Bytes.toBytes(row[0]));
                put.addColumn(Bytes.toBytes(row[1]), Bytes.toBytes(row[2]), Bytes.toBytes(row[3]));
                putList.add(put);
            }
            table.put(putList);
            table.close();
            System.out.println("batch row insert success.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    /**
     * get数据
     * @param tbName
     * @param rowKey
     * @param colFamily
     * @param col
     */
    public void getData(String tbName, String rowKey, String colFamily, String col) {
        try {
            Table table = conn.getTable(TableName.valueOf(tbName));
            Get get = new Get(Bytes.toBytes(rowKey));
            // 获取指定列族数据
            // get.addFamily(Bytes.toBytes(colFamily));
            // 获取指定列数据
            // get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
            Result result = table.get(get);

            printResult(result);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量get
     * @param tableName
     * @param startRow
     * @param endRow
     */
    public void getBatchData(String tableName, String startRow, String endRow) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
//			scan.withStartRow(Bytes.toBytes(startRow));//.setStartRow(Bytes.toBytes(startRow));
//			scan.withStopRow(Bytes.toBytes(endRow));//.setStopRow(Bytes.toBytes(stopRow));
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                printResult(result);
            }
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印结果
     * @param result
     */
    private void printResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    /**
     * 删除数据
     * @param tbName
     * @param rowKey
     * @return
     */
    public int delData(String tbName, String rowKey) {
        int status = 0;
        try {
            Table table = conn.getTable(TableName.valueOf(tbName));
            // 删除整行
            Delete deleteAction = new Delete(Bytes.toBytes(rowKey));
            // 删除指定列族
            //deleteAction.addFamily(Bytes.toBytes(colFamily));
            // 删除指定列
            //deleteAction.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
            table.delete(deleteAction);
            table.close();
            System.out.println("row del success.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

    /**
     * 批量删除
     * @param tbName
     * @param rows
     * @return
     */
    public int delBatchData(String tbName, Object[] rows) {
        int status = 0;
        /*Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除指定列族
        // delete.addFamily(Bytes.toBytes(colFamily));
        // 删除指定列
        // delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        // 批量删除
         * List<Delete> deleteList = new ArrayList<Delete>();
         * deleteList.add(delete); table.delete(deleteList);
        table.close();*/
        return status;
    }

    /**
     * 更新数据
     * @param tbName
     * @param rowKey
     * @param colFamily
     * @param col
     * @param value
     * @return
     */
    public int updateData(String tbName, String rowKey, String colFamily, String col, String value) {
        int status = 0;
        status = insertData(tbName, rowKey, colFamily, col, value);
        return status;
    }

    /**
     * 扫描数据
     * @param tbName
     * @param start
     * @param end
     */
    public void scanData(String tbName, String start, String end) {

    }
}
