import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Node implements Serializable {

    private int maxEntriesPerNode;
    private Hashtable<Point, LinkedList<Comparable>> keys;
    private Node[] children;
    private Point boundary1, boundary2;
    private boolean leaf;
    public static int calls = 0;

    public Node(Comparable x1, Comparable y1, Comparable z1, Comparable x2, Comparable y2, Comparable z2, int maxEntriesPerNode) {
        this.maxEntriesPerNode = maxEntriesPerNode;
        boundary1 = new Point(x1, y1, z1);
        boundary2 = new Point(x2, y2, z2);
        keys = new Hashtable<>();
        leaf = true;
    }

    public void setChildren(Node[] children) {
        this.children = children;
    }

    public int getSize() {
        return keys.size();
    }

    public boolean isLeaf() {
        return leaf;
    }

    public void setLeaf(boolean leaf) {
        this.leaf = leaf;
    }

    public Point getBoundary1() {
        return boundary1;
    }

    public Point getBoundary2() {
        return boundary2;
    }

    public void setKeys(Hashtable<Point, LinkedList<Comparable>> keys) {
        this.keys = keys;
    }

    public Point getMidPoint() throws DBAppException {
        return new Point(getMid(getBoundary1().getX(), getBoundary2().getX()),
                getMid(getBoundary1().getY(), getBoundary2().getY()),
                getMid(getBoundary1().getZ(), getBoundary2().getZ()));
    }

    public Comparable getValuePlusOne(Comparable c) throws DBAppException {
        if (c instanceof Integer)
            return (Integer) c + 1;
        else if (c instanceof Double)
            return (Double) c + 1;
        else if (c instanceof Date) {
            String dt = c.toString();
            SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Calendar cal = Calendar.getInstance();
            try {
                cal.setTime(formatter.parse(dt));
            } catch (ParseException e) {
                throw new DBAppException();
            }
            cal.add(Calendar.DATE, 1);
            dt = sdf.format(cal.getTime());
            try {
                return sdf.parse(dt);
            } catch (ParseException e) {
                throw new DBAppException();
            }
        }
        else
            return getNextString(((String) c).toLowerCase());
    }

    public String getNextString(String str) {
        if (str == "")
            return "a";
        int i = str.length() - 1;
        while (str.charAt(i) == 'z' && i >= 0)
            i--;
        if (i == -1)
            str = str + 'a';
        else
            str = str.substring(0, i) +
                    (char) ((int) (str.charAt(i)) + 1) +
                    str.substring(i + 1);
        return str;
    }

    public Comparable getMid(Comparable first, Comparable second) throws DBAppException {
        if (first instanceof Integer) {
            return ((Integer) first + (Integer) second) / 2;
        } else if (first instanceof Double) {
            return ((Double) first + (Double) second) / 2;
        } else if (first instanceof Date) {
            long difference_In_Time
                    = ((Date) second).getTime() - ((Date) first).getTime();
            long difference_In_Days
                    = (difference_In_Time
                    / (1000 * 60 * 60 * 24));
            String dt = first.toString();
            SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Calendar c = Calendar.getInstance();
            try {
                c.setTime(formatter.parse(dt));
            } catch (ParseException e) {
                throw new DBAppException();
            }
            c.add(Calendar.DATE, (int) (difference_In_Days / 2));
            dt = sdf.format(c.getTime());
            try {
                return sdf.parse(dt);
            } catch (ParseException e) {
                throw new DBAppException();
            }
        } else {
            String s1 = ((String) first).toLowerCase(), s2 = ((String) second).toLowerCase();
            StringBuilder sb1 = new StringBuilder(s1), sb2 = new StringBuilder(s2);
            if (s1.length() < s2.length())
                sb1.append(s2.substring(s1.length()));
            if (s2.length() < s1.length())
                sb2.append(s1.substring(s2.length()));
            return getMiddleString(sb1.toString(), sb2.toString());
        }
    }


    public String getMiddleString(String s1, String s2) {
        int n = s1.length();
        int[] a1 = new int[n + 1];

        for (int i = 0; i < n; i++) {
            a1[i + 1] = (int) s1.charAt(i) - 97
                    + (int) s2.charAt(i) - 97;
        }
        for (int i = n; i >= 1; i--) {
            a1[i - 1] += a1[i] / 26;
            a1[i] %= 26;
        }
        for (int i = 0; i <= n; i++) {
            if ((a1[i] & 1) != 0) {
                if (i + 1 <= n)
                    a1[i + 1] += 26;
            }
            a1[i] = a1[i] / 2;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++)
            sb.append((char) (a1[i] + 97));
        return sb.toString();
    }


    public Hashtable<Point, LinkedList<Comparable>> getKeys() {
        return keys;
    }

    public boolean isFull() {
        return keys.size() == maxEntriesPerNode;
    }

    public Comparable gotoNextLevel(Comparable x, Comparable y, Comparable z, Object obj, Operation op) throws DBAppException {
        Point mid = getMidPoint();
        int index = 0;
        if (x.compareTo(mid.getX()) <= 0) {
            if (y.compareTo(mid.getY()) <= 0) {
                if (z.compareTo(mid.getZ()) <= 0)
                    index = 0;
                else
                    index = 1;
            } else {
                if (z.compareTo(mid.getZ()) <= 0)
                    index = 2;
                else
                    index = 3;
            }
        } else {
            if (y.compareTo(mid.getY()) <= 0) {
                if (z.compareTo(mid.getZ()) <= 0)
                    index = 4;
                else
                    index = 5;
            } else {
                if (z.compareTo(mid.getZ()) <= 0)
                    index = 6;
                else
                    index = 7;
            }
        }
        switch (op) {
            case INSERT:
                children[index].insert(x, y, z, obj);
                return true;
        }
        return false;
    }

    public void insert(Comparable x, Comparable y, Comparable z, Object obj) throws DBAppException {
        if (isLeaf()) {
            if (!isFull()) {
                boolean exists = false;
                for(Map.Entry<Point, LinkedList<Comparable>> entry : keys.entrySet()){
                    Point key = entry.getKey();
                    LinkedList list = entry.getValue();
                    if(key.equals(new Point(x, y, z))) {
                        list.add((Comparable) obj);
                        exists = true;
                    }
                }
                if(!exists){
                    if(obj instanceof LinkedList)
                        getKeys().put(new Point(x, y, z), (LinkedList<Comparable>) obj);
                    else if(obj instanceof RowReference) {
                        LinkedList<Comparable> list = new LinkedList<>();
                        list.add((Comparable) obj);
                        getKeys().put(new Point(x, y, z), list);
                    }
                }
            } else {
                setLeaf(false);
                if (children == null)
                    children = new Node[8];
                Point mid = getMidPoint();
                Comparable x1 = getBoundary1().getX(), x2 = getBoundary2().getX(), y1 = getBoundary1().getY(),
                        y2 = getBoundary2().getY(), z1 = getBoundary1().getZ(), z2 = getBoundary2().getZ();
                children[0] = new Node(x1, y1, z1, mid.getX(), mid.getY(), mid.getZ(), maxEntriesPerNode);
                children[1] = new Node(x1, y1, getValuePlusOne(mid.getZ()), mid.getX(), mid.getY(), z2, maxEntriesPerNode);
                children[2] = new Node(x1, getValuePlusOne(mid.getY()), z1, mid.getX(), y2, mid.getZ(), maxEntriesPerNode);
                children[3] = new Node(x1, getValuePlusOne(mid.getY()), getValuePlusOne(mid.getZ()), mid.getX(), y2, z2, maxEntriesPerNode);
                children[4] = new Node(getValuePlusOne(mid.getX()), y1, z1, x2, mid.getY(), mid.getZ(), maxEntriesPerNode);
                children[5] = new Node(getValuePlusOne(mid.getX()), y1, getValuePlusOne(mid.getZ()), x2, mid.getY(), z2, maxEntriesPerNode);
                children[6] = new Node(getValuePlusOne(mid.getX()), getValuePlusOne(mid.getY()), z1, x2, y2, mid.getZ(), maxEntriesPerNode);
                children[7] = new Node(getValuePlusOne(mid.getX()), getValuePlusOne(mid.getY()), getValuePlusOne(mid.getZ()), x2, y2, z2, maxEntriesPerNode);
                for (Map.Entry<Point, LinkedList<Comparable>> entry : getKeys().entrySet()) {
                    Point location = entry.getKey();
                    LinkedList<Comparable> val = entry.getValue();
                    gotoNextLevel(location.getX(), location.getY(), location.getZ(), val, Operation.INSERT);
                }
                getKeys().clear();
                gotoNextLevel(x, y, z, obj, Operation.INSERT);
            }
        } else
            gotoNextLevel(x, y, z, obj, Operation.INSERT);
    }

    public void delete(Comparable x, Comparable y, Comparable z, Comparable clusteringKey) throws DBAppException {
        if (isLeaf()) {
                Point p = null;
                RowReference reference = null;
         outer: for (Map.Entry<Point, LinkedList<Comparable>> entry : getKeys().entrySet()) {
                    p = entry.getKey();
                    if (p != null && p.getX().equals(x) && p.getY().equals(y) && p.getZ().equals(z)) {
                        for(Comparable reference1 : entry.getValue()){
                            if(((RowReference) reference1).getClusteringKey().equals(clusteringKey)) {
                                reference = (RowReference) reference1;
                                break outer;
                            }
                        }
                    }
                }
                if (p != null) {
                    if(reference != null)
                        getKeys().get(p).remove(reference);
                    if(getKeys().get(p).size() == 0)
                        getKeys().remove(p);
                }
        } else {
            Point mid = getMidPoint();
            int index = 0;
            if (x.compareTo(mid.getX()) <= 0) {
                if (y.compareTo(mid.getY()) <= 0) {
                    if (z.compareTo(mid.getZ()) <= 0)
                        index = 0;
                    else
                        index = 1;
                } else {
                    if (z.compareTo(mid.getZ()) <= 0)
                        index = 2;
                    else
                        index = 3;
                }
            } else {
                if (y.compareTo(mid.getY()) <= 0) {
                    if (z.compareTo(mid.getZ()) <= 0)
                        index = 4;
                    else
                        index = 5;
                } else {
                    if (z.compareTo(mid.getZ()) <= 0)
                        index = 6;
                    else
                        index = 7;
                }
            }
            children[index].delete(x, y, z, clusteringKey);
            boolean delete = true;
            for (Node child : children) {
                if (child.getSize() != 0 || !child.isLeaf()) {
                    delete = false;
                    break;
                }
            }
            if (delete) {
                children = null;
                setLeaf(true);
            }
        }
    }

    public void get(Comparable clusteringKey, int x, Vector<Integer> res) throws DBAppException {
        if (isLeaf()) {
            for (Map.Entry<Point, LinkedList<Comparable>> entry : getKeys().entrySet()) {
                for (Comparable reference : entry.getValue()) {
                    if (((RowReference) reference).getClusteringKey().equals(clusteringKey)) {
                        res.add(((RowReference) reference).getPageNO());
                        return;
                    }
                }
            }
        } else {
            Point p = getMidPoint();
            if (x == 0) {
                if (clusteringKey.compareTo(p.getX()) <= 0) {
                    for (int i = 0; i <= 3; i++)
                        children[i].get(clusteringKey, x, res);
                } else {
                    for (int i = 4; i <= 7; i++)
                        children[i].get(clusteringKey, x, res);
                }
            } else if (x == 1) {
                if (clusteringKey.compareTo(p.getY()) <= 0) {
                    children[0].get(clusteringKey, x, res);
                    children[1].get(clusteringKey, x, res);
                    children[4].get(clusteringKey, x, res);
                    children[5].get(clusteringKey, x, res);
                } else {
                    children[2].get(clusteringKey, x, res);
                    children[3].get(clusteringKey, x, res);
                    children[6].get(clusteringKey, x, res);
                    children[7].get(clusteringKey, x, res);
                }
            } else {
                if (clusteringKey.compareTo(p.getZ()) <= 0) {
                    children[0].get(clusteringKey, x, res);
                    children[2].get(clusteringKey, x, res);
                    children[4].get(clusteringKey, x, res);
                    children[6].get(clusteringKey, x, res);
                } else {
                    children[1].get(clusteringKey, x, res);
                    children[3].get(clusteringKey, x, res);
                    children[5].get(clusteringKey, x, res);
                    children[7].get(clusteringKey, x, res);
                }
            }
        }
    }


    public Comparable get(Comparable clusteringKey) throws DBAppException{
        if(isLeaf()) {
            for (Map.Entry<Point, LinkedList<Comparable>> entry : getKeys().entrySet()) {
                for (Comparable reference : entry.getValue()){
                    if(((RowReference) reference).getClusteringKey().equals(clusteringKey))
                        return reference;
                }
            }
        }
        else {
            RowReference reference;
            for (int i = 0; i < 8; i++) {
                reference = (RowReference) children[i].get(clusteringKey);
                if (reference != null)
                    return reference;
            }
        }
        return null;
    }

    public boolean compare(Point p, Comparable x, Comparable y, Comparable z, String[] op){
        return process(p.getX(), x, op[0]) && process(p.getY(), y, op[1]) && process(p.getZ(), z, op[2]);
    }

    public boolean process(Comparable a, Comparable b, String op){
        switch (op){
            case "=": return a.equals(b);
            case "!=": return !a.equals(b);
            case ">": return a.compareTo(b) > 0;
            case "<": return a.compareTo(b) < 0;
            case ">=": return a.compareTo(b) >= 0;
            case "<=": return a.compareTo(b) <= 0;
        }
        return false;
    }

    public void get(Comparable x, Comparable y, Comparable z, String[] op, LinkedList<LinkedList<Comparable>> list) throws DBAppException {
        if(isLeaf()){
            for(Map.Entry<Point, LinkedList<Comparable>> entry : getKeys().entrySet()){
                Point p = entry.getKey();
                if(compare(p,x,y,z,op)) {
                    list.add(entry.getValue());
                }
            }
        }
        else{
            Point mid = getMidPoint();
            Vector<Integer> index = new Vector<>();
            if(!op[0].equals("=")){
                if(!op[1].equals("=")){
                    if(!op[2].equals("=")){
                        // all raneg
                        for (int i = 0; i <= 7; i++)
                            index.add(i);
                    }
                    else{
                        // x y range
                        if(z.compareTo(mid.getZ()) <= 0){
                            for (int i = 0; i <= 7; i+=2)
                                index.add(i);
                        }
                        else{
                            for (int i = 1; i <= 7; i+=2)
                                index.add(i);
                        }
                    }
                }
                else {
                    if (!op[2].equals("=")) {
                        // x z range
                        if (y.compareTo(mid.getY()) <= 0) {
                            index.add(0);
                            index.add(1);
                            index.add(4);
                            index.add(5);
                        } else {
                            index.add(2);
                            index.add(3);
                            index.add(6);
                            index.add(7);
                        }
                    } else {
                        // x range
                        if (y.compareTo(mid.getY()) <= 0) {
                            if (z.compareTo(mid.getZ()) <= 0) {
                                index.add(0);
                                index.add(4);
                            } else {
                                index.add(1);
                                index.add(5);
                            }
                        } else {
                            if (z.compareTo(mid.getZ()) <= 0) {
                                index.add(2);
                                index.add(6);
                            } else {
                                index.add(3);
                                index.add(7);
                            }
                        }
                    }
                }
            }
            else{
                if(!op[1].equals("=")){
                    if(!op[2].equals("=")) {
                        // y z range
                        if (x.compareTo(mid.getX()) <= 0) {
                            for (int i = 0; i <= 3; i++) {
                                index.add(i);
                            }
                        } else {
                            for (int i = 4; i <= 7; i++) {
                                index.add(i);
                            }
                        }
                    }
                    else{
                        // y range
                        if (x.compareTo(mid.getX()) <= 0) {
                            if (z.compareTo(mid.getZ()) <= 0) {
                                index.add(0);
                                index.add(2);
                            } else {
                                index.add(1);
                                index.add(3);
                            }
                        } else {
                            if (z.compareTo(mid.getZ()) <= 0) {
                                index.add(4);
                                index.add(6);
                            } else {
                                index.add(5);
                                index.add(7);
                            }
                        }
                    }
                }
                else{
                    if(!op[2].equals("=")) {
                        // z range
                        if (x.compareTo(mid.getX()) <= 0) {
                            if (y.compareTo(mid.getY()) <= 0) {
                                index.add(0);
                                index.add(1);
                            } else {
                                index.add(2);
                                index.add(3);
                            }
                        } else {
                            if (y.compareTo(mid.getY()) <= 0) {
                                index.add(4);
                                index.add(5);
                            } else {
                                index.add(6);
                                index.add(7);
                            }
                        }
                    }
                    else{
                        // all exact
                        if (x.compareTo(mid.getX()) <= 0) {
                            if (y.compareTo(mid.getY()) <= 0) {
                                if (z.compareTo(mid.getZ()) <= 0)
                                    index.add(0);
                                else
                                    index.add(1);
                            } else {
                                if (z.compareTo(mid.getZ()) <= 0)
                                    index.add(2);
                                else
                                    index.add(3);
                            }
                        } else {
                            if (y.compareTo(mid.getY()) <= 0) {
                                if (z.compareTo(mid.getZ()) <= 0)
                                    index.add(4);
                                else
                                    index.add(5);
                            } else {
                                if (z.compareTo(mid.getZ()) <= 0)
                                    index.add(6);
                                else
                                    index.add(7);
                            }
                        }
                    }
                }
            }
            for (int i : index)
                 children[i].get(x, y, z, op, list);
        }
    }
}
