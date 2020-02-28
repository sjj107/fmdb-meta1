package com.fiberhome.fmdb.meta.test;

import com.alibaba.fastjson.JSON;
import com.fiberhome.fmdb.common.CommonUtil;
import com.fiberhome.fmdb.common.Constant;
import com.fiberhome.fmdb.common.LoadConfFile;
import com.fiberhome.fmdb.meta.bean.TableInfo;
import com.fiberhome.fmdb.meta.bean.UDCTInfo;
import com.fiberhome.fmdb.meta.factory.FmdbMetaFactory;
import com.fiberhome.fmdb.meta.tool.IFMDBMetaClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

/**
 * @Description 入库测试
 * @Author sjj
 * @Date 20/01/02 下午 02:28
 **/
public class Import_ReadTest {
    private static String[] data = {"粤Z87PD21", "450803511@qq.com", "321088199011157118", "127.0.0.1", "F48E389C0256", "17368704996"};
    private static String dbName = "fhorc";
    private static String tblName = "localtest";
    private static IFMDBMetaClient metaClient;
    private static TableInfo tableInfo;
    private static Configuration conf = new Configuration();
    private static String path = "D:\\data\\fmdb_udct\\text.orc";

    public static void main(String[] args) throws IOException {
//        LoadConfFile.loadLog4j("conf/log4j.properties");
        metaClient = FmdbMetaFactory.INSTANCE.getMetaClient();
        tableInfo = metaClient.getTableInfo(dbName, tblName);
        importData();
        readData();
    }

    private static void importData() throws IOException {
        File file = new File(path);
        file.delete();
        String s = CommonUtil.INSTANCE.genTableStruct(tableInfo.getCols());
        TypeDescription shceme = TypeDescription.fromString(s);
        String json = getjson();
        conf.set(Constant.CONF_KEY, json);
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf).setSchema(shceme);
        VectorizedRowBatch rowBatch = shceme.createRowBatch();

        Writer writer = OrcFile.createWriter(new Path(path), writerOptions);
        CommonUtil.PutRecordMsg msg = CommonUtil.INSTANCE.putRecord(data, rowBatch, tableInfo);
        if (!msg.isSuccess()) {
            System.out.println(msg.getFalseInfo());
        }
        if (rowBatch.size != 0) {
            writer.addRowBatch(rowBatch);
        }
        writer.close();
    }

    private static void readData() throws IOException {
        String json = getjson();
        conf.set(Constant.CONF_KEY, json);
        Reader reader = OrcFile.createReader(new Path(path), OrcFile.readerOptions(conf));
        System.out.println("File schema: " + reader.getSchema());
        System.out.println("Row count: " + reader.getNumberOfRows());

        // Pick the schema we want to read using schema evolution
        TypeDescription readSchema = TypeDescription.fromString(CommonUtil.INSTANCE.genTableStruct(tableInfo.getCols())); // struct<z:int,y:string,x:bigint>
        // Read the row data
        VectorizedRowBatch batch = readSchema.createRowBatch();
        RecordReader rowIterator = reader.rows(reader.options().schema(readSchema));
        BytesColumnVector carnum = (BytesColumnVector) batch.cols[0];
        BytesColumnVector email = (BytesColumnVector) batch.cols[1];
        BytesColumnVector idcard = (BytesColumnVector) batch.cols[2];
        BytesColumnVector ip = (BytesColumnVector) batch.cols[3];
        BytesColumnVector mac = (BytesColumnVector) batch.cols[4];
        BytesColumnVector telephone = (BytesColumnVector) batch.cols[5];
        while (rowIterator.nextBatch(batch)) {
            for (int row = 0; row < batch.size; ++row) {
//            System.out.println("y: " + new String(y.vector[row]));
                System.out.println("carnum: " + carnum.toString(row));
                System.out.println("email: " + email.toString(row));
                System.out.println("idcard: " + idcard.toString(row));
                System.out.println("ip: " + ip.toString(row));
                System.out.println("mac: " + mac.toString(row));
                System.out.println("telephone: " + telephone.toString(row));
  /*          y.fill(y.vector[row]);
            System.out.println("y: " + y.toString(row));*/

            }
        }
        rowIterator.close();
    }

    private static String getjson() {
        ArrayList<UndefineEncodingBean> lists = new ArrayList<>();
        Set<String> allUDCTNames = metaClient.getAllUDCTNames();
        for (String udctName : allUDCTNames) {
            UDCTInfo udctInfo = metaClient.getUDCTInfo(udctName);
            UndefineEncodingBean undefineEncodingBean = new UndefineEncodingBean(udctInfo.getUdct_name(), udctInfo.getWriter(), udctInfo.getRead(), udctInfo.getBase_type().getDesc());
            lists.add(undefineEncodingBean);
        }
        UndefineEncodingJsonObj undefineEncodingJsonObj = new UndefineEncodingJsonObj();
        undefineEncodingJsonObj.setList(lists);
        String json = JSON.toJSONString(undefineEncodingJsonObj);
        return json;
    }

}
