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
import com.xxdb.data.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.*;
import java.util.*;

public class DolphinDBReader extends Reader {

    private static final String DOLPHINDB_DATAX_READER_VERSION = "1.30.22.2";
    private static String TABLE_HANDLE = "";

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration readerConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++)
                configurations.add(readerConfig);

            return configurations;
        }

        @Override
        public void init() {
            this.readerConfig = this.getPluginJobConf();
            this.validateParameter();
            if(readerConfig.getString(Key.DB_PATH) == null || readerConfig.getString(Key.DB_PATH).isEmpty())
                TABLE_HANDLE = readerConfig.getString(Key.TABLE_NAME);
            else
                TABLE_HANDLE = String.format("loadTable(\"%s\",`%s)", readerConfig.getString(Key.DB_PATH), readerConfig.getString(Key.TABLE_NAME));
            LOG.info("dolphindbreader params:{}", this.readerConfig.toJSON());
        }

        @Override
        public void destroy() {

        }

        private void validateParameter() {
            this.readerConfig.getNecessaryValue(Key.HOST, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.readerConfig.getNecessaryValue(Key.PORT, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.readerConfig.getNecessaryValue(Key.PWD, DolphinDbWriterErrorCode.REQUIRED_VALUE);
            this.readerConfig.getNecessaryValue(Key.USER_ID, DolphinDbWriterErrorCode.REQUIRED_VALUE);
        }
    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration readerConfig = null;
        private DBConnection dbConnection = null;
        private String functionSql = "";
        private List<String> cols = null;
        private String dbName = "";
        private String tbName = "";
        private String querySql = "";

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("Start to read DolphinDB.");
            try {
                BasicTable bt = null;
                if (!this.functionSql.equals(""))
                    // if set 'Table' or 'Where' param, use functionSql to run.
                    bt = (BasicTable) dbConnection.run(this.functionSql);
                else if (!this.querySql.equals("")) {
                    // if set 'querySql' param, disbale 'Table' and 'Where' param if setted, use querySql to run.
                    bt = (BasicTable) dbConnection.run(this.querySql);
                    initCols(bt);
                }

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
                    for (String col : this.cols) {
                        if (Objects.isNull(bt.getColumn(col)))
                            continue;

                        Entity.DATA_TYPE dataType = bt.getColumn(col).getDataType();
                        switch (dataType) {
                            case DT_FLOAT:
                                BasicFloatVector floatVec = (BasicFloatVector)  bt.getColumn(col);
                                column = floatVec.isNull(i) ? new DoubleColumn((Float)null) : new DoubleColumn(floatVec.getFloat(i));
                                break;
                            case DT_DOUBLE:
                                BasicDoubleVector doubleVec = (BasicDoubleVector) bt.getColumn(col);
                                column = doubleVec.isNull(i) ? new DoubleColumn((Double)null) : new DoubleColumn(doubleVec.getDouble(i));
                                break;
                            case DT_BOOL:
                                BasicBooleanVector booleanVec = (BasicBooleanVector) bt.getColumn(col);
                                column = booleanVec.isNull(i) ? new BoolColumn((Boolean) null) : new BoolColumn(booleanVec.getBoolean(i));
                                break;
                            case DT_DATE:
                                BasicDateVector dateVec = (BasicDateVector) bt.getColumn(col);
                                if( dateVec.isNull(i)  ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(dateVec.getDate(i).atTime(LocalTime.MIDNIGHT).atZone(ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_DATETIME:
                                BasicDateTimeVector dateTimeVec = (BasicDateTimeVector) bt.getColumn(col);
                                if( dateTimeVec.isNull(i) ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(dateTimeVec.getDateTime(i).atZone( ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_TIME:
                                BasicTimeVector timeVec = (BasicTimeVector) bt.getColumn(col);
                                column = timeVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(timeVec.getString(i));
                                break;
                            case DT_TIMESTAMP:
                                BasicTimestampVector timeStampVec = (BasicTimestampVector)  bt.getColumn(col);
                                if( timeStampVec.isNull(i) ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(timeStampVec.getTimestamp(i).atZone( ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_NANOTIME:
                                BasicNanoTimeVector nanoTimeVec = (BasicNanoTimeVector) bt.getColumn(col);
                                column = nanoTimeVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(nanoTimeVec.getString(i));
                                break;
                            case DT_NANOTIMESTAMP:
                                BasicNanoTimestampVector nanoTimestampVec = (BasicNanoTimestampVector)  bt.getColumn(col);
                                if( nanoTimestampVec.isNull(i) ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(nanoTimestampVec.getNanoTimestamp(i).atZone( ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_BYTE:
                                BasicByteVector byteVec = (BasicByteVector) bt.getColumn(col);
                                column = byteVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn((long)byteVec.getByte(i));
                                break;
                            case DT_LONG:
                                BasicLongVector longVec = (BasicLongVector)  bt.getColumn(col);
                                column = longVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn(longVec.getLong(i));
                                break;
                            case DT_SHORT:
                                BasicShortVector shortVec = (BasicShortVector)  bt.getColumn(col);
                                column = shortVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn((long)shortVec.getShort(i));
                                break;
                            case DT_INT:
                                BasicIntVector intVec = (BasicIntVector) bt.getColumn(col);
                                column = intVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn(intVec.getInt(i));
                                break;
                            case DT_UUID:
                                BasicUuidVector uuidVector = (BasicUuidVector) bt.getColumn(col);
                                column = uuidVector.isNull(i) ? new StringColumn((String) null) : new StringColumn(uuidVector.get(i).getString());
                                break;
                            case DT_BLOB:
                            case DT_STRING:
                                BasicStringVector stringVec = (BasicStringVector) bt.getColumn(col);
                                column = stringVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(stringVec.getString(i));
                                break;
                            case DT_SYMBOL:
                                Vector symbolVec = bt.getColumn(col);
                                column = symbolVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(symbolVec.getString(i));
                                break;
                            case DT_COMPLEX:
                                BasicComplexVector complexVec = (BasicComplexVector) bt.getColumn(col);
                                column = complexVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(complexVec.getString(i));
                                break;
                            case DT_DATEHOUR:
                                BasicDateHourVector dateHourVec = (BasicDateHourVector) bt.getColumn(col);
                                if( dateHourVec.isNull(i) ){
                                    column = new DateColumn((Date) null);
                                }else{
                                    column = new DateColumn(Date.from(dateHourVec.getDateHour(i).atZone( ZoneId.systemDefault()).toInstant()));
                                }
                                break;
                            case DT_DURATION:
                                BasicDurationVector durationVec = (BasicDurationVector) bt.getColumn(col);
                                column = durationVec.isNull(i) ? new LongColumn((Long) null) : new LongColumn(durationVec.getString(i));
                                break;
                            case DT_INT128:
                                BasicInt128Vector int128Vec = (BasicInt128Vector) bt.getColumn(col);
                                column = int128Vec.isNull(i) ? new StringColumn((String) null) : new StringColumn(int128Vec.getString(i));
                                break;
                            case DT_IPADDR:
                                BasicIPAddrVector ipaddrVec = (BasicIPAddrVector) bt.getColumn(col);
                                column = ipaddrVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(ipaddrVec.getString(i));
                                break;
                            case DT_MINUTE:
                                BasicMinuteVector minuteVec = (BasicMinuteVector) bt.getColumn(col);
                                column = minuteVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(minuteVec.getString(i));
                                break;
                            case DT_MONTH:
                                BasicMonthVector monthVec = (BasicMonthVector) bt.getColumn(col);
                                column = monthVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(monthVec.getString(i));
                                break;
                            case DT_POINT:
                                BasicPointVector pointVec = (BasicPointVector) bt.getColumn(col);
                                column = pointVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(pointVec.getString(i));
                                break;
                            case DT_SECOND:
                                BasicSecondVector secondVec = (BasicSecondVector) bt.getColumn(col);
                                column = secondVec.isNull(i) ? new StringColumn((String) null) : new StringColumn(secondVec.getString(i));
                                break;
                            case DT_VOID:
                                column = new StringColumn((String) null);
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
                LOG.error(ex.getMessage(), ex);
            }
            LOG.info("Value Send Success!!!!!!!");
        }

        private void initCols(JSONArray fieldArr){
            this.cols = new ArrayList<>();
            if (fieldArr.toString().equals("[]")){
                try {
                    BasicDictionary schema = (BasicDictionary) dbConnection.run(TABLE_HANDLE + ".schema()");
                    BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
                    BasicStringVector colNames = (BasicStringVector) colDefs.getColumn("name");
                    for (int i = 0; i < colDefs.rows(); i++)
                        this.cols.add(colNames.getString(i));
                }catch (Exception e){
                    LOG.error(e.getMessage(),e);
                }
            }else {
                for (int i = 0; i < fieldArr.size(); i++){
                    JSONObject field = fieldArr.getJSONObject(i);
                    this.cols.add(field.getString("name"));
                }
            }
        }

        private void initCols(BasicTable bt) {
            this.cols = new ArrayList<>();
            for (int i = 0; i < bt.columns(); i ++)
                this.cols.add(bt.getColumnName(i));
        }


        @Override
        public void init() {
            this.readerConfig = super.getPluginJobConf();
            String host = this.readerConfig.getString(Key.HOST);
            int port = this.readerConfig.getInt(Key.PORT);
            String userid = this.readerConfig.getString(Key.USER_ID);
            String pwd = this.readerConfig.getString(Key.PWD);

            dbConnection = new DBConnection();
            try {
                dbConnection.connect(host, port, userid, pwd);
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }

            String dbName = this.readerConfig.getString(Key.DB_PATH);
            String tbName = this.readerConfig.getString(Key.TABLE_NAME);
            this.dbName = dbName;
            this.tbName = tbName;
            String where = this.readerConfig.getString(Key.WHERE);
            List<Object> tableField = this.readerConfig.getList(Key.TABLE);
            JSONArray fieldArr = JSONArray.parseArray(JSON.toJSONString(tableField));
            String querySql = this.readerConfig.getString(Key.QUERY_SQL);
            if (Objects.nonNull(querySql) && !querySql.equals("")) {
                // if set 'querySql', disable 'Where' and  'Table' param.
                where = null;
                fieldArr = null;
            } else {
                initCols(fieldArr);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < cols.size(); i++) {
                    if (i != cols.size()-1)
                        sb.append(cols.get(i)).append(",");
                    else
                        sb.append(cols.get(i));
                }

                if (Objects.nonNull(where) && where.equals("")) {
                    if (fieldArr.toString().equals("[]"))
                        this.functionSql = String.format("select * from %s", TABLE_HANDLE);
                    else
                        this.functionSql = String.format("select " + sb + " from %s", TABLE_HANDLE);
                } else {
                    if (fieldArr.toString().equals("[]"))
                        this.functionSql = String.format("select * from %s where " + where, TABLE_HANDLE);
                    else
                        this.functionSql = String.format("select " + sb + " from %s where " + where, TABLE_HANDLE);
                }
            }

            this.querySql = querySql;
        }

        @Override
        public void destroy() {
            if (dbConnection != null){
                dbConnection.close();
            }
        }


    }
}
