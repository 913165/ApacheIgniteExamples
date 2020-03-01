package hello.world;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

public class IgniteNodeStart2 {
    public static void main(String[] args) throws IgniteException {
        Ignition.start("config2/helloworld-ignite.xml");
    }
}
