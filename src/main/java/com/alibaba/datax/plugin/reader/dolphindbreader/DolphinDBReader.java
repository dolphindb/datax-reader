package com.alibaba.datax.plugin.reader.dolphindbreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class DolphinDBReader extends Reader {

    public static class Job extends Reader.Job{
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration readerConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++){
                configurations.add(readerConfig);
            }
            return configurations;
        }

        @Override
        public void init() {
            this.readerConfig = this.getPluginJobConf();
            this.validateParameter();
            LOG.info("dolphindbreader params:{}", this.readerConfig.toJSON());
        }

        @Override
        public void destroy() {

        }

        /**
         *
         */
        private void validateParameter() {
            this.readerConfig.getNecessaryValue(Key.HOST, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.readerConfig.getNecessaryValue(Key.PORT, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.readerConfig.getNecessaryValue(Key.PWD, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.readerConfig.getNecessaryValue(Key.USER_ID, DolphinDbWriterErrorCode.REQUIRED_VALUE);
        }
    }

    public static class Task extends Reader.Task{

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration readerConfig = null;
        private DBConnection dbConnection = null;
        private String functionSql = "";
        private List<String> cols = null;

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("start to read DolphinDB");
            try {
                BasicTable bt = (BasicTable) dbConnection.run(functionSql);
                sendData(bt, recordSender);
            }catch (IOException e){
                LOG.error(e.getMessage(), e);
            }
        }



        public void sendData(BasicTable bt, RecordSender recordSender){
            try {
                for (int i = 0; i < bt.rows(); i++){
                    Column column = null;
                    Record record = recordSender.createRecord();
                    for (String one : this.cols){
                        Entity.DATA_TYPE dataType = bt.getColumn(one).getDataType();
                        switch (dataType) {
                            case DT_FLOAT:
                                BasicFloatVector floatVec = (BasicFloatVector)  bt.getColumn(one);
                                column = new DoubleColumn(floatVec.getFloat(i));
                                break;
                            case DT_DOUBLE:
                                BasicDoubleVector doubleVec = (BasicDoubleVector) bt.getColumn(one);
                                column = new DoubleColumn(doubleVec.getDouble(i));
                                break;
                            case DT_BOOL:
                                BasicBooleanVector booleanVec = (BasicBooleanVector) bt.getColumn(one);
                                column = new BoolColumn(booleanVec.getBoolean(i));
                                break;
                            case DT_DATE:
                                BasicDateVector dateVec = (BasicDateVector) bt.getColumn(one);
                                column = new DateColumn((long) dateVec.getInt(i));
                                break;
                            case DT_DATETIME:
                                BasicDateTimeVector dateTimeVec = (BasicDateTimeVector) bt.getColumn(one);
                                column = new DateColumn((long) dateTimeVec.getInt(i));
                                break;
                            case DT_TIME:
                                BasicTimeVector timeVec = (BasicTimeVector) bt.getColumn(one);
                                column = new StringColumn(timeVec.getString(i));
                                break;
                            case DT_TIMESTAMP:
                                BasicTimestampVector timeStampVec = (BasicTimestampVector)  bt.getColumn(one);
                                column = new DateColumn(timeStampVec.getLong(i));
                                break;
                            case DT_NANOTIME:
                                BasicNanoTimeVector nanoTimeVec = (BasicNanoTimeVector) bt.getColumn(one);
                                column = new StringColumn(nanoTimeVec.getString(i));
                                break;
                            case DT_NANOTIMESTAMP:
                                BasicNanoTimestampVector nanoTimestampVec = (BasicNanoTimestampVector)  bt.getColumn(one);
                                column = new StringColumn(nanoTimestampVec.getString(i));
                                break;
                            case DT_BYTE:
                                BasicByteVector byteVec = (BasicByteVector) bt.getColumn(one);
                                column = new LongColumn((long)byteVec.getByte(i));
                                break;
                            case DT_LONG:
                                BasicLongVector longVec = (BasicLongVector)  bt.getColumn(one);
                                column = new LongColumn(longVec.getLong(i));
                                break;
                            case DT_SHORT:
                                BasicShortVector shortVec = (BasicShortVector)  bt.getColumn(one);
                                column = new LongColumn((long)shortVec.getShort(i));
                                break;
                            case DT_INT:
                                BasicIntVector intVec = (BasicIntVector) bt.getColumn(one);
                                column = new LongColumn(intVec.getInt(i));
                                break;
                            case DT_UUID:
                                BasicUuidVector uuidVector = (BasicUuidVector) bt.getColumn(one);
                                column = new StringColumn(uuidVector.get(i).getString());
                                break;
                            case DT_STRING:
                                BasicStringVector stringVec = (BasicStringVector) bt.getColumn(one);
                                column = new StringColumn(stringVec.getString(i));
                                break;
                            case DT_SYMBOL:
                                BasicSymbolVector symbolVec = (BasicSymbolVector) bt.getColumn(one);
                                column = new StringColumn(symbolVec.getString(i));
                                break;
                            case DT_COMPLEX:
                                BasicComplexVector complexVec = (BasicComplexVector) bt.getColumn(one);
                                column = new StringColumn(complexVec.getString(i));
                                break;
                            case DT_DATEHOUR:
                                BasicDateHourVector dateHourVec = (BasicDateHourVector) bt.getColumn(one);
                                column = new DateColumn((long) dateHourVec.getInt(i));
                                break;
                            case DT_DURATION:
                                BasicDurationVector durationVec = (BasicDurationVector) bt.getColumn(one);
                                column = new LongColumn(durationVec.getString(i));
                                break;
                            case DT_INT128:
                                BasicInt128Vector int128Vec = (BasicInt128Vector) bt.getColumn(one);
                                column = new StringColumn(int128Vec.getString(i));
                                break;
                            case DT_IPADDR:
                                BasicIPAddrVector ipaddrVec = (BasicIPAddrVector) bt.getColumn(one);
                                column = new StringColumn(ipaddrVec.getString(i));
                                break;
                            case DT_MINUTE:
                                BasicMinuteVector minuteVec = (BasicMinuteVector) bt.getColumn(one);
                                column = new StringColumn(minuteVec.getString(i));
                                break;
                            case DT_MONTH:
                                BasicMonthVector monthVec = (BasicMonthVector) bt.getColumn(one);
                                column = new DateColumn((long) monthVec.getInt(i));
                                break;
                            case DT_POINT:
                                BasicPointVector pointVec = (BasicPointVector) bt.getColumn(one);
                                column = new StringColumn(pointVec.getString(i));
                                break;
                            case DT_SECOND:
                                BasicSecondVector secondVec = (BasicSecondVector) bt.getColumn(one);
                                column = new StringColumn(secondVec.getString(i));
                                break;
                            default:
                                LOG.info("Unsupported DataType!!!");
                                break;
                        }
                        record.addColumn(column);
                    }
                    recordSender.sendToWriter(record);
                }
            }catch (Exception ex){
                LOG.info(ex.getMessage());
            }
            LOG.info("Value Send Success!!!!!!!");
        }

        private void initCols(JSONArray fieldArr){
            this.cols = new ArrayList<>();
            for (int i = 0; i < fieldArr.size(); i++){
                JSONObject field = fieldArr.getJSONObject(i);
                String colName = field.getString("name");
                this.cols.add(colName);
            }
        }

        @Override
        public void init() {
            this.readerConfig = super.getPluginJobConf();
            String host = this.readerConfig.getString(Key.HOST);
            int port = this.readerConfig.getInt(Key.PORT);
            String userid = this.readerConfig.getString(Key.USER_ID);
            String pwd = this.readerConfig.getString(Key.PWD);

            String dbName = this.readerConfig.getString(Key.DB_PATH);
            String tbName = this.readerConfig.getString(Key.TABLE_NAME);
            String where = this.readerConfig.getString(Key.WHERE);
            this.functionSql = String.format("select * from loadTable('%s', '%s')", dbName, tbName);
            List<Object> tableField = this.readerConfig.getList(Key.TABLE);
            JSONArray fieldArr = JSONArray.parseArray(JSON.toJSONString(tableField));
            initCols(fieldArr);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < cols.size(); i++){
                if (i != cols.size()-1)
                    sb.append(cols.get(i)).append(",");
                else
                    sb.append(cols.get(i));
            }
            if (where.equals(""))
                this.functionSql = String.format("select" + sb.toString() + "from loadTable('%s', '%s')", dbName, tbName);
            else
                this.functionSql = String.format("select" + sb.toString() + "from loadTable('%s', '%s') where" + where, dbName, tbName);
            dbConnection = new DBConnection();
            try {
                dbConnection.connect(host, port, userid, pwd);
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }



        @Override
        public void destroy() {
            if (dbConnection != null){
                dbConnection.close();
            }
        }


    }
}
