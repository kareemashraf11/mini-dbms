import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class DBApp {
    private String path;
    private int pageMaxEntries;
    private int nodeMaxEntries;

    public void init( ) {
        path = "src/main/resources/data/";
        try {
                FileReader fr = new FileReader("src/main/resources/metadata.csv");
                BufferedReader br = new BufferedReader(fr);
                if (br.readLine() == null) {
                    FileWriter fw = new FileWriter("src/main/resources/metadata.csv");
                    fw.write("TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType,min,,max\n");
                    fw.close();
                }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax ) throws DBAppException {
        readConfigurationFile();
        verifyCreation(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
        Table t = new Table(strClusteringKeyColumn, htblColNameType);
        t.setPageMaxEntries(pageMaxEntries);
        new File(path + strTableName).mkdir();
        new File(path + strTableName + "/Indices").mkdir();
        Serializer.serialize(t, path + strTableName + "/" + strTableName + ".ser");
        writeTableMetaData(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);

    }

    public void verifyCreation(
            String strTableName,
            String strClusteringKeyColumn,
            Hashtable<String,String> htblColNameType,
            Hashtable<String,String> htblColNameMin,
            Hashtable<String,String> htblColNameMax) throws DBAppException {

        if (strTableName == null || strTableName.equals(""))
            throw new DBAppException("A table name must be provided");
        boolean f = validateTableExistence(strTableName);
        if (f)
            throw new DBAppException("Table with this name already exists");
        if (strClusteringKeyColumn == null || strClusteringKeyColumn.equals("") || !htblColNameType.containsKey(strClusteringKeyColumn))
            throw new DBAppException("Clustering key doesn't exist!");
        for (String key : htblColNameMin.keySet()) {
            if (!htblColNameMin.containsKey(key) || !htblColNameMax.containsKey(key))
                throw new DBAppException("A column must be specified with its datatype, minimum and maximum value");
        }
        for (String key : htblColNameMax.keySet()) {
            if (!htblColNameMin.containsKey(key) || !htblColNameMax.containsKey(key))
                throw new DBAppException("Minimum and maximum values must be provided for all columns!");
        }
        List<String> types = Arrays.asList("java.lang.Integer", "java.lang.Double", "java.lang.String", "java.util.Date");
        for (Entry<String, String> entry : htblColNameType.entrySet()) {
            String key = entry.getKey(), val = entry.getValue();
            if (!types.contains(val))
                throw new DBAppException("Datatype not supported!");
            if (!htblColNameMin.containsKey(key) || !htblColNameMax.containsKey(key))
                throw new DBAppException("A column must be specified with its datatype, minimum and maximum value");
            parseKey(htblColNameMin.get(key), htblColNameType.get(key));
            parseKey(htblColNameMax.get(key), htblColNameType.get(key));
        }
    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException {
        String clusteringKeyColumn = null;
        clusteringKeyColumn = verifyInsertion(strTableName, htblColNameValue);
        Table t = (Table) Serializer.deserialize(path + strTableName + "/" + strTableName + ".ser");
        t.insert(htblColNameValue, strTableName);
        Serializer.serialize(t, path + strTableName + "/" + strTableName + ".ser");
    }

    public String verifyInsertion(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException {
        if (strTableName == null || strTableName.equals(""))
            throw new DBAppException("A table name must be provided");
        boolean f = validateTableExistence(strTableName);
        if(!f)
            throw new DBAppException("Table with this name doesn't exists");
        Object[] tableData = readTableMetaData(strTableName);
        String clusteringKey = (String) tableData[0];
        Hashtable<String, String> htblColNameType = (Hashtable<String, String>) tableData[1];
        Hashtable<String, Object> htblColNameMin = (Hashtable<String, Object>) tableData[2];
        Hashtable<String, Object> htblColNameMax = (Hashtable<String, Object>) tableData[3];
        for(String key : htblColNameValue.keySet()){
            if(!htblColNameType.containsKey(key))
                throw new DBAppException("This column doesn't exist");
        }
        for(String key : htblColNameType.keySet()){
            if(!htblColNameValue.containsKey(key))
                throw new DBAppException("Null values are not supported");
        }
        if (!htblColNameValue.containsKey(clusteringKey))
            throw new DBAppException("No value provided for the primary key!");
        checkDataBoundaries(htblColNameType, htblColNameMin, htblColNameMax,  htblColNameValue, true);
        return clusteringKey;
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue ) throws DBAppException {
        Object[] arr = verifyUpdate(strTableName, strClusteringKeyValue, htblColNameValue);
        Table t = (Table) Serializer.deserialize(path + strTableName + "/" + strTableName + ".ser");
        t.update((Comparable) arr[0], htblColNameValue, strTableName);
        Serializer.serialize(t, path + strTableName + "/" + strTableName + ".ser");
    }

    public Object[] verifyUpdate(String strTableName,
                                                  String strClusteringKeyValue,
                                                  Hashtable<String,Object> htblColNameValue) throws DBAppException {
        if (strTableName == null || strTableName.equals(""))
            throw new DBAppException("A table name must be provided");
        if (strClusteringKeyValue == null || strClusteringKeyValue.equals(""))
            throw new DBAppException("A primary key value must be provided");
        boolean f = validateTableExistence(strTableName);
        if(!f)
            throw new DBAppException("Table with this name doesn't exists");
        Object[] tableData = readTableMetaData(strTableName);
        String clusteringKeyColumn = (String) tableData[0];
        Hashtable<String, String> htblColNameType = (Hashtable<String, String>) tableData[1];
        Hashtable<String, Object> htblColNameMin = (Hashtable<String, Object>) tableData[2];
        Hashtable<String, Object> htblColNameMax = (Hashtable<String, Object>) tableData[3];
        for(String key : htblColNameValue.keySet()){
            if(key.equals(clusteringKeyColumn))
                throw new DBAppException("Can't update primary key value");
            if(!htblColNameType.containsKey(key))
                throw new DBAppException("This column doesn't exist");
        }
        Comparable clusteringKey = parseKey(strClusteringKeyValue, htblColNameType.get(clusteringKeyColumn));
        checkDataBoundaries(htblColNameType, htblColNameMin, htblColNameMax, htblColNameValue, true);
        return new Object[] {clusteringKey, clusteringKeyColumn};
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException {
        String clusteringKeyColumn = verifyDeletion(strTableName, htblColNameValue);
        Table t = (Table) Serializer.deserialize(path + strTableName + "/" + strTableName + ".ser");
        t.delete(htblColNameValue, strTableName, clusteringKeyColumn);
        Serializer.serialize(t, path + strTableName + "/" + strTableName + ".ser");
    }

    public String verifyDeletion(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (strTableName == null || strTableName.equals(""))
            throw new DBAppException("A table name must be provided");
        boolean f = validateTableExistence(strTableName);
        if(!f)
            throw new DBAppException("Table with this name doesn't exists");
        Object[] tableData = readTableMetaData(strTableName);
        String clusteringKeyColumn = (String) tableData[0];
        Hashtable<String, String> htblColNameType = (Hashtable<String, String>) tableData[1];
        Hashtable<String, Object> htblColNameMin = (Hashtable<String, Object>) tableData[2];
        Hashtable<String, Object> htblColNameMax = (Hashtable<String, Object>) tableData[3];
        for(String key : htblColNameValue.keySet()){
            if(!htblColNameType.containsKey(key))
                throw new DBAppException("This column doesn't exist");
        }
        checkDataBoundaries(htblColNameType, htblColNameMin, htblColNameMax, htblColNameValue, false);
        return clusteringKeyColumn;
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators) throws DBAppException{
        String tableName = verifySelection(arrSQLTerms, strarrOperators);
        Table t = (Table) Serializer.deserialize(path+tableName+"/"+tableName+".ser");
        Iterator it = t.select(arrSQLTerms, strarrOperators);
        Serializer.serialize(t, path+tableName+"/"+tableName+".ser");
        return it;
    }

    public String verifySelection(SQLTerm[] arrSQLTerms,
                                String[] strarrOperators) throws DBAppException{
        if(arrSQLTerms.length == 0)
            throw new DBAppException();
        if(arrSQLTerms.length != strarrOperators.length + 1)
            throw new DBAppException();
        for(String s : strarrOperators){
            if(!s.equalsIgnoreCase("AND") && !s.equalsIgnoreCase("OR") && !s.equalsIgnoreCase("XOR"))
                throw new DBAppException();
        }
        String tableName = arrSQLTerms[0].get_strTableName();
        for(SQLTerm term : arrSQLTerms){
            if(!tableName.equals(term.get_strTableName()))
                throw new DBAppException("Joins are not supported");
        }
        if(!validateTableExistence(tableName))
            throw new DBAppException("Table doesn't exist");
        Object[] tableData = readTableMetaData(tableName);
        Hashtable<String, String> htblColNameType = (Hashtable<String, String>) tableData[1];
        HashSet<String> operators = new HashSet<>(Arrays.asList("=", "!=", ">", ">=", "<", "<="));
        for(SQLTerm term : arrSQLTerms){
            if(!operators.contains(term.get_strOperator()))
                throw new DBAppException();
            if(!htblColNameType.containsKey(term.get_strColumnName()))
                throw new DBAppException("Column doesn't exist");
            if(term.get_objValue() instanceof Integer && !htblColNameType.get(term.get_strColumnName()).equals("java.lang.Integer"))
                throw new DBAppException();
            else if(term.get_objValue() instanceof Double && !htblColNameType.get(term.get_strColumnName()).equals("java.lang.Double"))
                throw new DBAppException();
            else if(term.get_objValue() instanceof String && !htblColNameType.get(term.get_strColumnName()).equals("java.lang.String"))
                throw new DBAppException();
            else if(term.get_objValue() instanceof Date && !htblColNameType.get(term.get_strColumnName()).equals("java.util.Date"))
                throw new DBAppException();
        }

        return tableName;
    }

    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException{
        Hashtable<String, Object[]> colInfo = verifyIndexCreation(strTableName, strarrColName);
        Table t = (Table) Serializer.deserialize(path + strTableName+"/"+strTableName+".ser");

        Octree index = new Octree(t.getNoOfIndices() + 1, strarrColName, (Comparable) colInfo.get(strarrColName[0])[1], (Comparable) colInfo.get(strarrColName[1])[1], (Comparable) colInfo.get(strarrColName[2])[1],
                (Comparable) colInfo.get(strarrColName[0])[2], (Comparable) colInfo.get(strarrColName[1])[2], (Comparable) colInfo.get(strarrColName[2])[2],
                                  nodeMaxEntries);
        t.setNoOfIndices(t.getNoOfIndices()+1);
        updateTableMetaData(strTableName, strarrColName);
        populateIndex(index, t, strTableName);
        Serializer.serialize(index,path+strTableName+"/Indices/"+strTableName+"Index"+(t.getNoOfIndices()-1)+".ser");
        Serializer.serialize(t, path + strTableName+"/"+strTableName+".ser");
    }

    public Hashtable<String, Object[]> verifyIndexCreation(String strTableName,
                                                           String[] strarrColName) throws DBAppException{
        if(strarrColName.length != 3)
            throw new DBAppException();
        if(!validateTableExistence(strTableName))
            throw new DBAppException();
        Object[] tableData = readTableMetaData(strTableName);
        Hashtable<String, Object[]> colInfo = new Hashtable<>();
        for(String colName : strarrColName){
            Hashtable<String, String> colNameType = (Hashtable<String, String>) tableData[1];
            Hashtable<String, Object> colNameMin = (Hashtable<String, Object>) tableData[2];
            Hashtable<String, Object> colNameMax = (Hashtable<String, Object>) tableData[3];
            Hashtable<String, Boolean> colNameIndex = (Hashtable<String, Boolean>) tableData[4];
            if(!colNameType.containsKey(colName))
                throw new DBAppException();
            if(colNameIndex.get(colName))
                throw new DBAppException("There is an index built on column "+ colName);
            colInfo.put(colName, new Object[]{colNameType.get(colName), colNameMin.get(colName), colNameMax.get(colName)});
        }
        return colInfo;
    }

    public void populateIndex(Octree index, Table t, String strTableName) throws DBAppException {
        for(Page p : t.getPages()){
            p.setRows((Vector<Record>) Serializer.deserialize(p.getPath()));
            for(Record rec : p.getRows()){
                int[] colIndices = new int[3];
                for (int i = 0; i < index.getColumns().length; i++)
                    colIndices[i] = rec.getColumns().indexOf(index.getColumns()[i]);
                index.insert((Comparable) rec.getData().get(colIndices[0]), (Comparable) rec.getData().get(colIndices[1]), (Comparable) rec.getData().get(colIndices[2]), new RowReference(p.getId(), (Comparable) rec.getData().get(0)));
            }
            Serializer.serialize(p.getRows(), p.getPath());
            p.setRows(null);
        }
    }

    public void writeTableMetaData(String strTableName,
                              String strClusteringKeyColumn,
                              Hashtable<String,String> htblColNameType,
                              Hashtable<String,String> htblColNameMin,
                              Hashtable<String,String> htblColNameMax) throws DBAppException {
        try {
            FileReader fr = new FileReader("src/main/resources/metadata.csv");
            BufferedReader br = new BufferedReader(fr);
            StringBuilder sb = new StringBuilder();
            String s;
            while ((s = br.readLine()) != null){
                sb.append(s);
                sb.append('\n');
            }
            FileWriter fw = new FileWriter("src/main/resources/metadata.csv");
            for (String c : htblColNameType.keySet()) {
                sb.append(strTableName);
                sb.append(',');
                sb.append(c);
                sb.append(',');
                sb.append(htblColNameType.get(c));
                sb.append(',');
                sb.append(strClusteringKeyColumn.equals(c));
                sb.append(',');
                sb.append("null");
                sb.append(',');
                sb.append(htblColNameMin.get(c));
                sb.append(',');
                sb.append(htblColNameMax.get(c));
                sb.append('\n');
            }
            fw.write(sb.toString());
            fw.close();
        } catch (IOException e) {
            throw new DBAppException();
        }
    }

    public Object[] readTableMetaData(String strTableName) throws DBAppException {
        FileReader fr;
        try {
            fr = new FileReader("src/main/resources/metadata.csv");
        } catch (FileNotFoundException e) {
            throw new DBAppException();
        }
        BufferedReader br = new BufferedReader(fr);
        String tmp;
        String strClusteringKeyColumn = null;
        Hashtable<String,String> htblColNameType = new Hashtable<>();
        Hashtable<String,Object> htblColNameMin = new Hashtable<>();
        Hashtable<String,Object> htblColNameMax = new Hashtable<>();
        Hashtable<String,Boolean> htblColNameIndex = new Hashtable<>();
        try {
            while ((tmp = br.readLine()) != null) {
                String[] arr = tmp.split(",");
                if (arr[0].equals(strTableName)) {
                    if (arr[3].equals("true"))
                        strClusteringKeyColumn = arr[1];
                    Comparable min = parseKey(arr[5], arr[2]), max = parseKey(arr[6], arr[2]);
                    htblColNameType.put(arr[1], arr[2]);
                    htblColNameMin.put(arr[1], min);
                    htblColNameMax.put(arr[1], max);
                    htblColNameIndex.put(arr[1], arr[4].equals("true")? true : false);
                }
            }
        }
        catch (IOException e){
            throw new DBAppException();
        }
        return new Object[]{strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax, htblColNameIndex};

    }

    public void updateTableMetaData(String strTableName,
                                    String[] strarrColName) throws DBAppException {
        try {
            StringBuilder metadata = new StringBuilder();
            FileReader fr = new FileReader("src/main/resources/metadata.csv");
            BufferedReader br = new BufferedReader(fr);
            String row;
            while((row = br.readLine()) != null){
                String[] rowArr = row.split(",");
                if(strTableName.equals(rowArr[0])){
                    StringBuilder sb = new StringBuilder();
                    for(String col : strarrColName) {
                        if (col.equals(rowArr[1])) {
                            for (int i = 0; i < rowArr.length; i++) {
                                if (i == 4)
                                    sb.append(rowArr[1]+"Col,");
                                else if (i == rowArr.length - 1)
                                    sb.append(rowArr[i]);
                                else
                                    sb.append(rowArr[i] + ",");
                            }
                        }
                    }
                    if(!strarrColName[0].equals(rowArr[1]) && !strarrColName[1].equals(rowArr[1]) && !strarrColName[2].equals(rowArr[1]))
                        sb.append(row);
                    metadata.append(sb.toString()+'\n');
                }
                else
                    metadata.append(row+'\n');
            }
            FileWriter fw = new FileWriter("src/main/resources/metadata.csv");
            fw.write(metadata.toString());
            fw.close();
        }
        catch (IOException e){
            throw new DBAppException();
        }
    }

    public boolean validateTableExistence(String strTableName) throws DBAppException {
        boolean exists = false;
        File f = new File(path + strTableName);
        FileReader fr;
        try {
            fr = new FileReader("src/main/resources/metadata.csv");
        } catch (FileNotFoundException e) {
            throw new DBAppException();
        }
        try {
            BufferedReader br = new BufferedReader(fr);
            String tmp;
            while ((tmp = br.readLine()) != null) {
                String[] arr = tmp.split(",");
                if (arr[0].equals(strTableName)) {
                    exists = true;
                    break;
                }
            }
        }
        catch (IOException e){
            throw new DBAppException();
        }
        return exists && f.exists();
    }


    public void checkDataBoundaries(Hashtable<String, String> htblColNameType,
                                    Hashtable<String, Object> htblColNameMin,
                                    Hashtable<String, Object> htblColNameMax,
                                    Hashtable<String, Object> htblColNameValue,
                                    boolean e) throws DBAppException{
        for(Entry<String, Object> entry : htblColNameValue.entrySet()){
            String key = entry.getKey();
            Object val = entry.getValue();
            String type = htblColNameType.get(key);
            Object min = htblColNameMin.get(key);
            Object max = htblColNameMax.get(key);
            if(val instanceof Integer && type.equals("java.lang.Integer")){
                Integer val1 = (Integer) val, val2 = (Integer) min, val3 = (Integer) max;
                if(val1.compareTo(val2)<0 || val1.compareTo(val3)>0)
                    if(e)
                        throw new DBAppException("Values must be wihthin the table's specified ranges");
                    else
                        return;
            }
            else if(val instanceof Double && type.equals("java.lang.Double")){
                Double val1 = (Double) val, val2 = (Double) min, val3 = (Double) max;
                if(val1.compareTo(val2)<0 || val1.compareTo(val3)>0)
                    if(e)
                        throw new DBAppException("Values must be wihthin the table's specified ranges");
                    else
                        return;
            }
            else if(val instanceof String && type.equals("java.lang.String")){
                String val1 = (String) val, val2 = (String) min, val3 = (String) max;
                if(val1.compareTo(val2)<0 || val1.compareTo(val3)>0)
                    if(e)
                        throw new DBAppException("Values must be wihthin the table's specified ranges");
                    else
                        return;
            }
            else if(val instanceof Date && type.equals("java.util.Date")){
                Date val1 = (Date) val, val2 = (Date) min, val3 = (Date) max;
                if(val1.compareTo(val2)<0 || val1.compareTo(val3)>0)
                    if(e)
                        throw new DBAppException("Values must be wihthin the table's specified ranges");
                    else
                        return;
            }
            else{
                throw new DBAppException("Datatypes must be compatible with the table's specified datatypes");
            }

        }
    }

    public void readConfigurationFile() throws DBAppException{

        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        try (FileInputStream reader = new FileInputStream(fileName)) {
            prop.load(reader);
        }
        catch (IOException e) {
            throw new DBAppException();
        }
        pageMaxEntries = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
        nodeMaxEntries = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
    }

    public static Comparable parseKey(String key, String type) throws DBAppException {
        if(type.equals("java.lang.Integer")) {
            try {
                return Integer.parseInt(key);
            } catch (Exception e) {
                throw new DBAppException("Problem with parsing this value");
            }
        }
        else if(type.equals("java.lang.Double")) {
            try {
                return Double.parseDouble(key);
            } catch (Exception e) {
                throw new DBAppException("Problem with parsing this value");
            }
        }
        else if(type.equals("java.util.Date")) {
            try {
                return new SimpleDateFormat("yyyy-MM-dd").parse(key);
            } catch (Exception e) {
                throw new DBAppException("Problem with parsing the date");
            }
        }
        return key;
    }
}
