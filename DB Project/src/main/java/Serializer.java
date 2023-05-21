import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.IOException;

public class Serializer implements Serializable {


    public static void serialize(Serializable obj, String path) throws DBAppException {
        try {
            FileOutputStream file = new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(file);
            out.writeObject(obj);
            out.close();
            file.close();
        }
        catch (IOException e){
            throw new DBAppException();
        }
    }

    public static Object deserialize(String path) throws DBAppException{
        Object obj;
        try {
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
            obj = in.readObject();
            in.close();
            file.close();
        }
        catch (ClassNotFoundException e){
            throw new DBAppException("class not found");
        }
        catch (IOException e){
            throw new DBAppException("io");
        }
        return obj;
    }

}
