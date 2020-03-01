package hello.world;

import java.util.Iterator;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;

public class IgniteZooKeeperCnnect {

	private static Iterator<Entry<Integer, String>> iterator;

	public static void main(String[] args) throws InterruptedException {
		ZookeeperDiscoverySpi zkDiscoSpi = new ZookeeperDiscoverySpi();
		zkDiscoSpi.setZkConnectionString("127.0.0.1:2181");
		zkDiscoSpi.setSessionTimeout(30_000);
		zkDiscoSpi.setZkRootPath("/apacheIgnite");
		zkDiscoSpi.setJoinTimeout(10_000);
		IgniteConfiguration cfg = new IgniteConfiguration();
		// Override default discovery SPI.
		cfg.setDiscoverySpi(zkDiscoSpi);
		try (Ignite ignite = Ignition.start(cfg)) {

			IgniteCache<Integer, String> cache = ignite.getOrCreateCache("empCache");
			cache.put(101, "John Smith");
			cache.put(102, "Juan Carlos");
			cache.put(103, "Mike Jones");
			cache.put(104, "William jack");
			cache.put(105, "Thomas Harry");

			iterator = cache.iterator();
			while (iterator.hasNext()) {
				Entry<Integer, String> entry = iterator.next();
				System.out.println("Key : " + entry.getKey() + " :Value : " + entry.getValue());
			}
			Thread.sleep(999999999);
		}
	}
}