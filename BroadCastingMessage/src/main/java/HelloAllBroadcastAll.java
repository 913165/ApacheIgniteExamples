import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

public class HelloAllBroadcastAll {
	public static void main(String[] args) {
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setPeerClassLoadingEnabled(true);
		cfg.setClientMode(true);
		try (Ignite ignite = Ignition.start(cfg)) {
			IgniteCompute compute = ignite.compute();
			// broadcast the computation to all nodes
			compute.broadcast(() -> {
				System.out.println("Hello Everyone");
			});
		}
	}
}