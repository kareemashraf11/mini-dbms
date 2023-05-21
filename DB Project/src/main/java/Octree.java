import java.io.Serializable;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Vector;

public class Octree implements Serializable {

    private Node root;
    private int id;
    private String[] columns;

    public Octree(int id, String[] columns, Comparable x1, Comparable y1, Comparable z1, Comparable x2, Comparable y2, Comparable z2, int maxEntriesPerNode){
        root = new Node(x1, y1, z1, x2, y2, z2, maxEntriesPerNode);
        this.id = id;
        this.columns = columns;
    }

    public void insert(Comparable x, Comparable y, Comparable z, Comparable obj) throws DBAppException {
        root.insert(x, y, z, obj);
    }

    public void delete(Comparable x, Comparable y, Comparable z, Comparable clusteringKey) throws DBAppException{
        root.delete(x, y, z, clusteringKey);
    }

    public void update(Comparable oldX, Comparable oldY, Comparable oldZ, Comparable newX, Comparable newY, Comparable newZ, Comparable clusteringKey, RowReference reference) throws DBAppException {
        root.delete(oldX, oldY, oldZ, clusteringKey);
        root.insert(newX, newY, newZ, reference);
    }

    public void clear(){
        root.setLeaf(true);
        root.setKeys(new Hashtable<>());
        root.setChildren(null);
    }

    public LinkedList<LinkedList<Comparable>> get(Comparable x, Comparable y, Comparable z, String[] op) throws DBAppException {
        LinkedList<LinkedList<Comparable>> result = new LinkedList<>();
        root.get(x, y, z, op, result);
        return result;
    }

    public int get(Comparable clusteringKey, int x) throws DBAppException {
        Vector<Integer> res = new Vector<>();
        root.get(clusteringKey, x, res);
        if(res.size() == 1)
            return res.get(0);
        return -1;
    }

    public String[] getColumns() {
        return columns;
    }
}
