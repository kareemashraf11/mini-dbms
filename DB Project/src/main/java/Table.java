import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {
    private int pageMaxEntries;
    private int NoOfIndices;
    private Vector<Page> pages;
    private Vector<String> columns;


    public Table(String strClusteringKeyColumn,
                 Hashtable<String,String> htblColNameType){
        pages = new Vector<>();
        columns = new Vector<>();
        for(String c : htblColNameType.keySet()){
            boolean flag = strClusteringKeyColumn.equals(c);
            if(flag)
                columns.add(0, c);
            else
                columns.add(c);
        }
        NoOfIndices = 0;
    }

    public void setNoOfIndices(int noOfIndices) {
        NoOfIndices = noOfIndices;
    }

    public int getNoOfIndices() {
        return NoOfIndices;
    }

    public void setPageMaxEntries(int pageMaxEntries) {
        this.pageMaxEntries = pageMaxEntries;
    }

    public int getPageMaxEntries() {
        return pageMaxEntries;
    }

    public Vector<Page> getPages() {
        return pages;
    }

    public Vector<String> getColumns() {
        return columns;
    }

    public int getNumberOfPages(){
        return pages.size();
    }

    public void insert(Hashtable<String, Object> htblColNameValue, String strTableName) throws DBAppException{
        Vector<Object> v = new Vector<>();
        for(String c : columns){
            v.add(htblColNameValue.get(c));
        }
        Record rec = new Record();
        rec.setRecord(v, columns);
        if(getNumberOfPages() == 0){
            Page p = new Page(getNumberOfPages(), strTableName);
            insertHelper(p, rec, htblColNameValue, strTableName);
            pages.add(p);
        }
        else{
            int destination = getDestinationPageID(rec.getData().get(0));
            if(destination >=0 && destination < getNumberOfPages()) {
                for (int i = destination; i < getNumberOfPages(); i++) {
                    Page tmp = pages.get(i);
                    insertHelper(tmp, rec, htblColNameValue, strTableName);
                    if (tmp.getSize() > getPageMaxEntries()) {
                        rec = tmp.removeFromPageEnd();
                        deleteFromIndex(tmp, rec, strTableName);
                    } else
                        return;
                }
                Page newPage = new Page(getNumberOfPages(), strTableName);
                insertHelper(newPage, rec, htblColNameValue, strTableName);
                pages.add(newPage);
            }
        }
    }

    public void insertHelper(Page p, Record rec, Hashtable<String, Object> htblColNameValue, String strTableName) throws DBAppException{
        RowReference reference = p.insertIntoPage(rec);
        for (int i = 0; i < getNoOfIndices(); i++) {
            Octree index = (Octree) Serializer.deserialize("src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
            if(rec.getData().get(0).equals(12) || rec.getData().get(0).equals(4))
            index.insert((Comparable) rec.getData().get(rec.getColumns().indexOf(index.getColumns()[0])), (Comparable) rec.getData().get(rec.getColumns().indexOf(index.getColumns()[1])), (Comparable) rec.getData().get(rec.getColumns().indexOf(index.getColumns()[2])), reference);
            Serializer.serialize(index, "src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
        }
    }

    public void deleteFromIndex(Page p, Record rec, String strTableName) throws DBAppException{
        for (int i = 0; i < getNoOfIndices(); i++) {
            Octree index = (Octree) Serializer.deserialize("src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
            index.delete((Comparable) rec.getData().get(rec.getColumns().indexOf(index.getColumns()[0])), (Comparable) rec.getData().get(rec.getColumns().indexOf(index.getColumns()[1])), (Comparable) rec.getData().get(rec.getColumns().indexOf(index.getColumns()[2])), (Comparable) rec.getData().get(0));
            Serializer.serialize(index, "src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
        }
    }

    public void delete(Hashtable<String, Object> htblColNameValue, String strTableName, String clusteringKeyColumn) throws DBAppException {
        if(getNumberOfPages() == 0)
            throw new DBAppException("The table is empty");
        if(htblColNameValue.size() == 0) {
            for (Page p : pages)
                p.deletePage();
            pages.removeAllElements();
            for (int i = 0; i < getNoOfIndices(); i++) {
                Octree index = (Octree) Serializer.deserialize("src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
                index.clear();
                Serializer.serialize(index, "src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
            }
            return;
        }
        Vector<Record> deleted = new Vector<>();
        boolean used = false;
        for (int i = 0; i < getNoOfIndices(); i++) {
            Octree index = (Octree) Serializer.deserialize("src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
            used = false;
            if (htblColNameValue.containsKey(index.getColumns()[0]) && htblColNameValue.containsKey(index.getColumns()[1]) && htblColNameValue.containsKey(index.getColumns()[2])) {
                LinkedList<LinkedList<Comparable>> list = index.get((Comparable) htblColNameValue.get(index.getColumns()[0]), (Comparable) htblColNameValue.get(index.getColumns()[1]), (Comparable) htblColNameValue.get(index.getColumns()[2]), new String[]{"=", "=", "="});
                if(list == null)
                    return;
                if(getColumns().get(0).equals(index.getColumns()[0]) || getColumns().get(0).equals(index.getColumns()[1]) || getColumns().get(0).equals(index.getColumns()[2])){
                    int pageNo = -1;
                    for(LinkedList<Comparable> list1 : list){
                        if(list1 != null && list1.size() > 0) {
                            pageNo = ((RowReference) list1.getFirst()).getPageNO();
                            break;
                        }
                    }
                    if(pageNo == -1)
                        throw new DBAppException();
                    Page p = getPages().get(pageNo);
                    if(p.deleteFromPagePK(htblColNameValue, getColumns().get(0), index, deleted)){
                        p.deletePage();
                        getPages().remove(p);
                    }
                }
                else {
                    for(LinkedList<Comparable> list1 : list) {
                        if(list1 != null) {
                            LinkedList<Comparable> tmp = (LinkedList<Comparable>) list1.clone();
                            for (Comparable reference : tmp) {
                                RowReference reference1 = (RowReference) reference;
                                Page p = getPages().get(reference1.getPageNO());
                                if (p.deleteFromPage(htblColNameValue, index, deleted)) {
                                    p.deletePage();
                                    getPages().remove(p);
                                }
                            }
                        }
                    }
                }
                used = true;
            }
            Serializer.serialize(index, "src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
            if (used)
                break;
        }

        if(!used) {

            if (htblColNameValue.containsKey(clusteringKeyColumn)) {
                int destination = getDestinationPageID(htblColNameValue.get(clusteringKeyColumn));
                if (destination >= 0 && destination < getNumberOfPages()) {
                    Page p = pages.get(destination);
                    if (p.deleteFromPagePK(htblColNameValue, clusteringKeyColumn, null, deleted)) {
                        pages.remove(p);
                        p.deletePage();
                    }
                }
            } else {
                Vector<Page> tmp = new Vector<>();
                for (Page p : getPages()) {
                    if (p.deleteFromPage(htblColNameValue, null, deleted)) {
                        tmp.add(p);
                        p.deletePage();
                    }
                }
                pages.removeAll(tmp);
            }
        }
        for (int i = 0; i < getNoOfIndices(); i++) {
            Octree index = (Octree) Serializer.deserialize("src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
            for(Record rec : deleted)
                index.delete((Comparable) rec.getData().get(rec.getColumns().indexOf(index.getColumns()[0])), (Comparable) rec.getData().get(rec.getColumns().indexOf(index.getColumns()[1])), (Comparable) rec.getData().get(rec.getColumns().indexOf(index.getColumns()[2])), (Comparable) rec.getData().get(0));
            Serializer.serialize(index, "src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
        }
    }

    public void update(Comparable clusteringKey,
                       Hashtable<String,Object> htblColNameValue,
                       String strTableName) throws DBAppException{

        if(getNumberOfPages() == 0)
            throw new DBAppException("The table is empty!");
        for (int i = 0; i < getNoOfIndices(); i++) {
            Octree index = (Octree) Serializer.deserialize("src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
            int idx = -1;
            if(getColumns().get(0).equals(index.getColumns()[0]))
                idx = index.get(clusteringKey, 0);
            else if(getColumns().get(0).equals(index.getColumns()[1]))
                idx = index.get(clusteringKey, 1);
            else if(getColumns().get(0).equals(index.getColumns()[2]))
                idx = index.get(clusteringKey, 2);
            Serializer.serialize(index, "src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
            if(idx != -1){
                Hashtable<String, Object> ht;
                Page p = pages.get(idx);
                ht = p.update(clusteringKey, htblColNameValue);
                updateIndex(strTableName, htblColNameValue, ht, clusteringKey, idx);
            }
        }
        int destination = getDestinationPageID(clusteringKey);
        Hashtable<String, Object> ht;
        if(destination >=0 && destination < getNumberOfPages()) {
            Page p = pages.get(destination);
            ht = p.update(clusteringKey, htblColNameValue);
            updateIndex(strTableName, htblColNameValue, ht, clusteringKey, destination);
        }
    }

    public void updateIndex(String strTableName, Hashtable<String, Object> htblColNameValue, Hashtable<String, Object> ht, Comparable clusteringKey, int pageNo) throws DBAppException{
        for (int i = 0; i < getNoOfIndices(); i++) {
            Octree index = (Octree) Serializer.deserialize("src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
            if(!htblColNameValue.containsKey(index.getColumns()[0]) && !htblColNameValue.containsKey(index.getColumns()[1]) && !htblColNameValue.containsKey(index.getColumns()[2]))
                continue;
            index.update((Comparable) ht.get(index.getColumns()[0]), (Comparable) ht.get(index.getColumns()[1]), (Comparable) ht.get(index.getColumns()[2]), (Comparable) htblColNameValue.getOrDefault(index.getColumns()[0], (Comparable) ht.get(index.getColumns()[0])), (Comparable) htblColNameValue.getOrDefault(index.getColumns()[1], (Comparable) ht.get(index.getColumns()[1])), (Comparable) htblColNameValue.getOrDefault(index.getColumns()[2], (Comparable) ht.get(index.getColumns()[2])), clusteringKey, new RowReference(pageNo, clusteringKey));
            Serializer.serialize(index, "src/main/resources/data/" + strTableName + "/Indices/" + strTableName + "Index" + i + ".ser");
        }
    }

    public Iterator select(SQLTerm[] arrSQLTerms,
                           String[] strarrOperators) throws DBAppException {
        Vector<Record> finalResult = new Vector<>();
        if(getNumberOfPages() == 0)
            throw new DBAppException();
        for (int i = 0; i < getNoOfIndices(); i++) {
            Octree index = (Octree) Serializer.deserialize("src/main/resources/data/" + arrSQLTerms[0].get_strTableName() + "/Indices/" + arrSQLTerms[0].get_strTableName() + "Index" + i + ".ser");
            boolean f = false;
            int[] arr = new int[3];
            Arrays.fill(arr, -1);
            for (int j = 0; j < arrSQLTerms.length; j++) {
                if(j < strarrOperators.length) {
                    if (!strarrOperators[j].equals("AND")) {
                        Arrays.fill(arr, -1);
                        continue;
                    }
                }
                if(arrSQLTerms[j].get_strColumnName().equals(index.getColumns()[0])) {
                    arr[0] = j;
                }
                else if(arrSQLTerms[j].get_strColumnName().equals(index.getColumns()[1])) {
                    arr[1] = j;
                }
                else if(arrSQLTerms[j].get_strColumnName().equals(index.getColumns()[2])) {
                    arr[2] = j;
                }
                else
                    Arrays.fill(arr, -1);

                if(arr[0] != -1 && arr[1] != -1 && arr[2] != -1) {
                    f = true;
                    break;
                }
            }
            if(f){
                LinkedList<LinkedList<Comparable>> result = index.get((Comparable) arrSQLTerms[arr[0]].get_objValue(), (Comparable) arrSQLTerms[arr[1]].get_objValue(), (Comparable) arrSQLTerms[arr[2]].get_objValue(), new String[]{arrSQLTerms[arr[0]].get_strOperator(), arrSQLTerms[arr[1]].get_strOperator(), arrSQLTerms[arr[2]].get_strOperator()});
                if(result != null) {
                    for (LinkedList<Comparable> list1 : result) {
                        if (list1 != null) {
                            for (Comparable reference : list1) {
                                int pageNo = ((RowReference) reference).getPageNO();
                                Comparable pK = ((RowReference) reference).getClusteringKey();
                                Page p = getPages().get(pageNo);
                                if(pK != null) {
                                    p.setRows((Vector<Record>) Serializer.deserialize(p.getPath()));
                                    finalResult.add(p.getRows().get(p.searchPK(pK)));
                                    Serializer.serialize(p.getRows(), p.getPath());
                                    p.setRows(null);
                                }
                            }
                        }
                    }
                }
                return finalResult.iterator();
            }

        }

        for(Page p : getPages()){
            finalResult.addAll(p.select(arrSQLTerms, strarrOperators));
        }
        return finalResult.iterator();
    }

    public int getDestinationPageID(Object clusteringKey){
        int lo = 0;
        int hi = pages.size()-1;
        while(lo<=hi){
            int mid = (lo+hi)>>1;
            Page p = pages.get(mid);
            Comparable key = (Comparable) clusteringKey;
            Object min = p.getMinimumValue(), max = p.getMaximumValue();
            if(key.compareTo(min)>=0 && key.compareTo(max)<=0
                    || pages.size()==mid+1 && key.compareTo(max)>=0
                    || mid==0 && key.compareTo(min)<=0
                    || key.compareTo(max)>=0 && mid + 1 < pages.size() && key.compareTo(pages.get(mid+1).getMinimumValue())<0)
                return mid;
            else if(key.compareTo(min)<=0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }
        return -1;
    }
}
