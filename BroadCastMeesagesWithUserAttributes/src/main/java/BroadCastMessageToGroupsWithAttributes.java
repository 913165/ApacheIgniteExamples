import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;

public class BroadCastMessageToGroupsWithAttributes {
	public static void main(String[] args) {
		IgniteConfiguration cfg = new IgniteConfiguration();
		cfg.setPeerClassLoadingEnabled(true);
		Ignite ignite = Ignition.start(cfg);
		IgniteCluster cluster = ignite.cluster();
		ClusterGroup groupWithAttributes = cluster.forAttribute("country", "usa");
		IgniteCompute fooAttributeCompute = ignite.compute(groupWithAttributes);
		fooAttributeCompute.broadcast(()->{
			System.out.println("message to coutry with usa");
		});
	}
}
