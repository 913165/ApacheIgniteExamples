import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

public class IgniteClient1 {
	public static void main(String[] args) throws InterruptedException {
		IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
		igniteConfiguration.setPeerClassLoadingEnabled(true);
		igniteConfiguration.setClientMode(true);
		Map<String, String> map = new HashMap<String, String>();
		map.put("country", "india");
		igniteConfiguration.setUserAttributes(map);
		Ignite ignite = Ignition.start(igniteConfiguration);
		while (true) {
			System.out.println("India node is running");
			Thread.sleep(10000);
		}
	}
}