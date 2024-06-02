package manager;
import index.bplusTree.BPlusTreeIndexFile;
import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise

        if (!check_file_exists(table_name)) {
            return null;
        }
        
        if(get_data_block(table_name, block_id)==null){
            return null;
        }

        int file_id = file_to_fileid.get(table_name);
        byte[] column =db.get_data(file_id,0);
        int numCol = (column[1] << 8) | (column[0] & 0xFF);

        int total_var_length=0;
        int total_fix_length=0;
        List<Integer> fix = new ArrayList<>();
        int[] coltype= new int[numCol];
        for(int i=0;i<numCol;i++){
            int offset = (column[2*i+3] << 8) | (column[2*i+2] & 0xFF);
            int type= column[offset] & 0xFF ;
            int name_length= column[offset+1] & 0xFF;
            byte[] colname_byte=db.get_data(file_id,0,offset+2,name_length);
            String colname = new String(colname_byte); 
            if(type==0){
                total_var_length+=1;
            }
            if(type==1 || type==3){
                total_fix_length=4;
                fix.add(total_fix_length);
            }
            if(type==2){
                total_fix_length=1;
                fix.add(total_fix_length);
            }
            if(type==4){
                total_fix_length=8;
                fix.add(total_fix_length);
            }
            
            coltype[i]=type;
        }

        byte[]data = get_data_block(table_name, block_id);
        int numRecords = (data[0] << 8) | (data[1] & 0xFF);
        List<Object[]> list = new ArrayList<>();
        for(int i=0;i<numRecords;i++){
            int recordoffset= (data[2*i+2] << 8) | (data[2*i+3] & 0xFF);
            Object[] obj = new Object[numCol];
            int temp=recordoffset+4*(total_var_length);
            for(int j=0;j<fix.size();j++){
                if(coltype[j]==1){
                    byte[] bytes=db.get_data(file_id,block_id,temp,4);
                    int value =convertBytesToT1(bytes,Integer.class);
                    obj[j]=value;
                }
                if(coltype[j]==2){
                    byte[] bytes=db.get_data(file_id,block_id,temp,1);
                    Boolean value =convertBytesToT1(bytes,Boolean.class);
                    obj[j]=value;
                }
                if(coltype[j]==3){
                    byte[] bytes=db.get_data(file_id,block_id,temp,4);
                    Float value =convertBytesToT1(bytes,Float.class);
                    obj[j]=value;
                }
                if(coltype[j]==4){
                    byte[] bytes=db.get_data(file_id,block_id,temp,8);
                    Double value =convertBytesToT1(bytes,Double.class);
                    obj[j]=value;
                }
                
                temp+=fix.get(j);
            }
            for(int j=0;j<total_var_length;j++){
                int temp1=recordoffset+4*j;
                int rcoffset= (data[temp1+1] << 8) | (data[temp1] & 0xFF);
                int rclength= (data[temp1+3] << 8) | (data[temp1+2] & 0xFF);
                byte[] rowbyte=db.get_data(file_id,block_id,recordoffset+rcoffset,rclength);
                String value = new String(rowbyte);
                obj[fix.size()+j]=value;
            }
            list.add(obj);

        }

        return list;
    }

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */
        // BPlusTreeIndexFile<T> file= new BPlusTreeIndexFile<>(order,"STRING");
        String index_file_name = table_name + "_" + column_name + "_index";
        if (check_index_exists(table_name, column_name)) {
            return false;
        }
        if (!check_file_exists(table_name)) {
            return false;
        }


        int file_id = file_to_fileid.get(table_name);
        byte[] column =db.get_data(file_id,0);

        int numCol = (column[1] << 8) | (column[0] & 0xFF);


        int fix_length=-1;
        int var_length=-1;
        int total_var_length=0;
        int total_fix_length=0;
        int coltype=-1;
        for(int i=0;i<numCol;i++){
            int offset = (column[2*i+3] << 8) | (column[2*i+2] & 0xFF);
            int type= column[offset] & 0xFF ;
            int name_length= column[offset+1] & 0xFF;
            byte[] colname_byte=db.get_data(file_id,0,offset+2,name_length);
            String colname = new String(colname_byte); 
            if(type==0){
                total_var_length+=1;
            }
        
            if (colname.equals(column_name)){
                if(type==0){
                    var_length=total_var_length;
                }
                else{
                    fix_length=total_fix_length;
                }
                coltype= type;
            }

            if(type==1 || type==3){
                total_fix_length+=4;
            }
            if(type==2){
                total_fix_length+=1;
            }
            if(type==4){
                total_fix_length+=8;
            }

        }



        if(coltype==0){
            BPlusTreeIndexFile<String> indexFile= new BPlusTreeIndexFile<String>(order, String.class);
    
            int block_id=1;
            while(get_data_block(table_name,block_id)!=null){
                byte[]data = get_data_block(table_name, block_id);
                int numRecords = (data[0] << 8) | (data[1] & 0xFF);
                for(int i=0;i<numRecords;i++){
                    int recordoffset= (data[2*i+2] << 8) | (data[2*i+3] & 0xFF);
                    int temp=recordoffset+4*(var_length-1);
                    int rcoffset= (data[temp+1] << 8) | (data[temp] & 0xFF);
                    int rclength= (data[temp+3] << 8) | (data[temp+2] & 0xFF);
                    byte[] rowbyte=db.get_data(file_id,block_id,recordoffset+rcoffset,rclength);
                    String value = new String(rowbyte);
                    indexFile.insert(value,block_id);
                }
                
                block_id+=1;
            }
            int index_file_id = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, index_file_id);
        }
        if(coltype==1){
            BPlusTreeIndexFile<Integer> indexFile= new BPlusTreeIndexFile<Integer>(order, Integer.class);
            int block_id=1;
            while(get_data_block(table_name,block_id)!=null){
                byte[]data = get_data_block(table_name, block_id);
                int numRecords = (data[0] << 8) | (data[1] & 0xFF);
                for(int i=0;i<numRecords;i++){
                    int recordoffset= (data[2*i+2] << 8) | (data[2*i+3] & 0xFF);
                    int temp=recordoffset+4*(total_var_length)+fix_length;
                    byte[] bytes=db.get_data(file_id,block_id,temp,4);
                    int value =convertBytesToT1(bytes,Integer.class);
                    indexFile.insert(value,block_id);
                }
                
                block_id+=1;
            }
            int index_file_id = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, index_file_id);
        }

        if(coltype==2){
            BPlusTreeIndexFile<Boolean> indexFile= new BPlusTreeIndexFile<Boolean>(order, Boolean.class);
            int block_id=1;
            while(get_data_block(table_name,block_id)!=null){
                byte[]data = get_data_block(table_name, block_id);
                int numRecords = (data[0] << 8) | (data[1] & 0xFF);
                for(int i=0;i<numRecords;i++){
                    int recordoffset= (data[2*i+2] << 8) | (data[2*i+3] & 0xFF);
                    int temp=recordoffset+4*(total_var_length)+fix_length;  
                    byte[] bytes=db.get_data(file_id,block_id,temp,1);
                    boolean value=convertBytesToT1(bytes,Boolean.class);
                    indexFile.insert(value,block_id);
                }
                
                block_id+=1;
            }
            int index_file_id = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, index_file_id);
        }

        if(coltype==3){
            BPlusTreeIndexFile<Float> indexFile= new BPlusTreeIndexFile<Float>(order, Float.class);
            int block_id=1;
            while(get_data_block(table_name,block_id)!=null){
                byte[]data = get_data_block(table_name, block_id);
                int numRecords = (data[0] << 8) | (data[1] & 0xFF);
                for(int i=0;i<numRecords;i++){
                    int recordoffset= (data[2*i+2] << 8) | (data[2*i+3] & 0xFF);
                    int temp=recordoffset+4*(total_var_length)+fix_length;  
                
                    byte[] bytes=db.get_data(file_id,block_id,temp,4);
                    float value =convertBytesToT1(bytes,Float.class);
                    indexFile.insert(value,block_id);
                }
                
                block_id+=1;
            }
            int index_file_id = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, index_file_id);
        }

        

        if(coltype==4){
            BPlusTreeIndexFile<Double> indexFile= new BPlusTreeIndexFile<Double>(order, Double.class);
            int block_id=1;
            while(get_data_block(table_name,block_id)!=null){
                byte[]data = get_data_block(table_name, block_id);
                int numRecords = (data[0] << 8) | (data[1] & 0xFF);
                for(int i=0;i<numRecords;i++){
                    int recordoffset= (data[2*i+2] << 8) | (data[2*i+3] & 0xFF);
                    int temp=recordoffset+4*(total_var_length)+fix_length;  
            
                    byte[] bytes=db.get_data(file_id,block_id,temp,8);
                    double value =convertBytesToT1(bytes,Double.class);
                    indexFile.insert(value,block_id);
                }
                
                block_id+=1;
            }
            int index_file_id = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, index_file_id);
        }

        return true;
    }

    


    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Check if the table exists
        if (!check_file_exists(table_name)) {
            return -1;
        }

        String index_file_name = table_name + "_" + column_name + "_index";
        if (!check_index_exists(table_name, column_name)) {
            return -1;
        }
        int file_id =file_to_fileid.get(index_file_name);
        int block_id= db.search_index(file_id,value);

        return block_id;
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
        
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

     private <T> T convertBytesToT1(byte[] bytes, Class<T> typeClass) {
        if (typeClass.equals(String.class)) {
            return typeClass.cast(new String(bytes));
        } else if (typeClass.equals(Integer.class)) {
            return typeClass.cast(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt());
        } else if (typeClass.equals(Boolean.class)) {
            return typeClass.cast(bytes[0] != 0);
        } else if (typeClass.equals(Float.class)) {
            return typeClass.cast(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat());
        } else if (typeClass.equals(Double.class)) {
            return typeClass.cast(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble());
        } else {
            // Handle unsupported types or return null
            return null;
        }
    }

}