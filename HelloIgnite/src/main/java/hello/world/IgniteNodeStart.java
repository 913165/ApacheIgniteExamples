package hello.world;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

public class IgniteNodeStart {
    public static void main(String[] args) throws IgniteException {
        Ignition.start("config/helloworld-ignite.xml");
    }
}
