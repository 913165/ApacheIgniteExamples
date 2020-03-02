import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;

public class BroadCastMessageToGroups {
	public static void main(String[] args) {
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setPeerClassLoadingEnabled(true);
		Ignite ignite = Ignition.start(cfg);
		IgniteCluster cluster = ignite.cluster();
		ClusterGroup cacheGroup = cluster.forCacheNodes("cache1");
		ClusterGroup dataGroup = cluster.forDataNodes("cache1");
		ClusterGroup clientGroup = cluster.forClientNodes("cache1");
		IgniteCompute cacheGroupCompute = ignite.compute(cacheGroup);
		IgniteCompute dataGroupCompute = ignite.compute(dataGroup);
		IgniteCompute clientGroupCompute = ignite.compute(clientGroup);
		cacheGroupCompute.broadcast(() -> {
			System.out.println("Message broadcasted to Cache Group Only");
		});
		dataGroupCompute.broadcast(() -> {
			System.out.println("Message broadcasted to data/server Group Only");
		});
		clientGroupCompute.broadcast(() -> {
			System.out.println("Message broadcasted to node with Client nodes");
		});
	}
}
