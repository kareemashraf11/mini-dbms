import java.io.File;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class Page implements Serializable, Comparable {

    private int id;
    private Vector<Record> rows;
    private String path;
    private int size;
    private Object minimumValue;
    private Object maximumValue;



    public Page(int id, String tableName){
        this.id = id;
        rows = new Vector<>();
        path = "src/main/resources/data/"+tableName+"/"+tableName+id+".ser";
        size = 0;
        minimumValue = maximumValue = null;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Vector<Record> getRows() {
        return rows;
    }

    public void setRows(Vector<Record> rows) {
        this.rows = rows;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Object getMinimumValue() {
        return minimumValue;
    }

    public void setMinimumValue(Object minimumValue) {
        this.minimumValue = minimumValue;
    }

    public Object getMaximumValue() {
        return maximumValue;
    }

    public void setMaximumValue(Object maximumValue) {
        this.maximumValue = maximumValue;
    }

    public RowReference insertIntoPage(Record rec) throws DBAppException{
        if(rows == null)
            rows = (Vector<Record>) Serializer.deserialize(path);
        int f = searchPK(rec.getData().get(0));
        if(f!=-1)
            throw new DBAppException("Record with this primary key already exists");
        int idx = getPreviousRecordPos(rec.getData().get(0));
        rows.add(idx+1, rec);
        size++;
        updateBoundaries();
        Serializer.serialize(rows, path);
        rows = null;
        return new RowReference(getId(), (Comparable) rec.getData().get(0));
    }

    public  int getPreviousRecordPos(Object o) {
        int lo = 0;
        int hi = getSize() - 1;
        while (lo <= hi) {
            int mid = (lo+hi)>>1;
            if (((Comparable) o).compareTo(rows.get(mid).getData().get(0))>0) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return hi;
    }

        public int searchPK(Object o) throws DBAppException {
            if (rows == null)
                rows = (Vector<Record>) Serializer.deserialize(path);
            int lo = 0;
            int hi = getSize() - 1;
            while (lo <= hi) {
                int mid = (lo + hi) >> 1;
                if (o.equals(rows.get(mid).getData().get(0)))
                    return mid;
                else if (((Comparable) o).compareTo(rows.get(mid).getData().get(0)) > 0)
                    lo = mid + 1;
                else
                    hi = mid - 1;
            }
            return -1;
        }

    public boolean deleteFromPagePK(Hashtable<String, Object> htblColNameValue, String clusteringKeyColumn, Octree index, Vector<Record> deleted) throws DBAppException{
        if (rows == null)
            rows = (Vector<Record>) Serializer.deserialize(path);
        int f = searchPK(htblColNameValue.get(clusteringKeyColumn));
        boolean match = true;
        if(f == -1)
            throw new DBAppException("Record doesn't exist");
        else {
            Record rec = rows.get(f);
            for(Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();
                int idx = -1;
                for (String col : rec.getColumns()) {
                    if (key.equals(col))
                        idx = rec.getColumns().indexOf(col);
                }
                if (!val.equals(rec.getData().get(idx))) {
                    match = false;
                    break;
                }
            }
            if(match) {
                deleted.add(rec);
                rows.remove(rec);
                size--;
                updateBoundaries();
                Serializer.serialize(rows, path);
                rows = null;
                if (getSize() == 0)
                    return true;
            }
        }
        return false;
    }




    public boolean deleteFromPage(Hashtable<String, Object> htblColNameValue, Octree index, Vector<Record> deleted) throws DBAppException {
        if (rows == null)
            rows = (Vector<Record>) Serializer.deserialize(path);
        Vector<Record> tmp = new Vector<>();
        for (Record rec : rows) {
            boolean f = true;
            for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();
                int i = -1;
                for (String col : rec.getColumns()) {
                    if (key.equals(col))
                        i = rec.getColumns().indexOf(col);
                }
                if (!val.equals(rec.getData().get(i))) {
                    f = false;
                    break;
                }
            }
            if (f) {
                deleted.add(rec);
                tmp.add(rec);
                size--;
            }
        }
            rows.removeAll(tmp);
            updateBoundaries();
            Serializer.serialize(rows, path);
            rows = null;
        if (getSize() == 0)
                return true;
        return false;

    }
    public Hashtable<String, Object> update(Comparable clusteringKey,
                       Hashtable<String,Object> htblColNameValue) throws DBAppException{
        if (rows == null)
            rows = (Vector<Record>) Serializer.deserialize(path);
        int f = searchPK(clusteringKey);
        Hashtable<String, Object> ht = new Hashtable<>();
        if(f == -1)
            throw new DBAppException("Record with this primary key doesn't exist");
        else {
            Record rec = rows.get(f);
            for (int i = 0; i < rec.getData().size(); i++)
                ht.put(rec.getColumns().get(i), rec.getData().get(i));
            for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();
                int index = -1;
                for (String col : rec.getColumns()) {
                    if (key.equals(col))
                        index = rec.getColumns().indexOf(col);
                }
                rec.getData().set(index, val);
            }
        }
        Serializer.serialize(rows, path);
        rows = null;
        return ht;
    }

    public Vector<Record> select(SQLTerm[] arrSQLTerms,
                                 String[] strarrOperators) throws DBAppException {
        if(rows == null)
            rows = (Vector<Record>) Serializer.deserialize(path);
        Vector<Record> res = new Vector<>();
        for(Record rec : getRows()){
            if(validate(rec, arrSQLTerms, strarrOperators))
                res.add(rec);
        }
        Serializer.serialize(rows, path);
        rows = null;
        return res;
    }

    public boolean validate(Record rec,
                            SQLTerm[] arrSQLTerms,
                            String[] strarrOperators){

        boolean selectRecord = validateCondition(rec, arrSQLTerms[0]);
        for(int i = 1; i < arrSQLTerms.length; i++){
            boolean f = validateCondition(rec, arrSQLTerms[i]);
            switch (strarrOperators[i-1].toUpperCase()) {
                case "AND": selectRecord&= f; break;
                case "OR": selectRecord|= f; break;
                case "XOR": selectRecord^= f; break;
            }
        }
        return selectRecord;
    }

    public boolean validateCondition(Record rec,
                       SQLTerm term){
        String colName = term.get_strColumnName();
        String operator = term.get_strOperator();
        Comparable val = (Comparable) term.get_objValue();
        int index = -1;
        for (String col : rec.getColumns()) {
            if (colName.equals(col))
                index = rec.getColumns().indexOf(col);
        }
        Object o = rec.getData().get(index);
        switch(operator){
            case "=": return o != null ? val.equals(o) : false;
            case "!=": return o != null ? !val.equals(o) : false;
            case ">": return o != null ? val.compareTo(o) < 0 : false;
            case "<": return o != null ? val.compareTo(o) > 0 : false;
            case ">=": return o != null ? val.compareTo(o) <= 0 : false;
            case "<=": return o != null ? val.compareTo(o) >= 0 : false;
        }
        return false;
    }

    public void deletePage(){
        new File(path).delete();
    }

    public Record removeFromPageEnd() throws DBAppException{
        if(rows == null)
            rows = (Vector<Record>) Serializer.deserialize(path);
        if(size == 0)
            return null;
        Record tmp = rows.remove(rows.size()-1);
        size--;
        updateBoundaries();
        Serializer.serialize(rows, path);
        rows = null;
        return tmp;
    }

    public void updateBoundaries(){
        if(rows != null && size > 0) {
            minimumValue = rows.get(0).getData().get(0);
            maximumValue = rows.get(size - 1).getData().get(0);
        }
    }

    public int compareTo(Object o) {
        return this.id - ((Page) o).id;
    }
}
