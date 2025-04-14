package main.java.storage;

import main.java.util.*;
import main.java.exception.*;
import main.java.index.*;
import main.java.query.*;

import java.io.Serializable;
import java.util.Vector;

public class Record implements Serializable, Comparable{

    private Vector<Object> data;
    private Vector<String> columns;

    public Record() {
    }

    public Vector<Object> getData() {
        return data;
    }

    public Vector<String> getColumns() { return columns; }

    public void setRecord(Vector<Object> data, Vector<String> columns) {
        this.data = data;
        this.columns = columns;
    }

    public int compareTo(Object o){
        Record rec = (Record) o;
        return ((Comparable) data.get(0)).compareTo(rec.getData().get(0));
    }

    public String toString(){
        return columns.toString() + "\n" + data.toString();
    }
}
