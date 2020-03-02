import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import com.fasterxml.uuid.Generators;

public class IgniteClient1 {
	public static void main(String[] args) throws InterruptedException {
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setPeerClassLoadingEnabled(true);
		cfg.setClientMode(true);
		Ignite ignite = Ignition.start(cfg);
		IgniteCache<String, String> cache = ignite.getOrCreateCache("cache1");
		while (true) {
			UUID uuid1 = Generators.timeBasedGenerator().generate();
			cache.put(uuid1.toString(), uuid1.toString());
			System.out.println(uuid1.toString());
			Thread.sleep(10000);
		}
	}
}