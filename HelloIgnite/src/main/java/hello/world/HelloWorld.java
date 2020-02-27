package hello.world;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

public class HelloWorld {

	private static Iterator<Entry<Integer, String>> iterator;

	public static void main(String[] args) {
		try (Ignite ignite = Ignition.start()) {

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

		}
	}
}