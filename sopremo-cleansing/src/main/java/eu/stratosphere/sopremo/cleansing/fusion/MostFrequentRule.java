package eu.stratosphere.sopremo.cleansing.fusion;

import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class MostFrequentRule extends ConflictResolution {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8375051813752883648L;

	private final Object2DoubleMap<IJsonNode> histogram = new Object2DoubleOpenHashMap<IJsonNode>();

	@Override
	public IJsonNode fuse(final IArrayNode values, final double[] weights, final FusionContext context) {
		this.histogram.clear();
		for (int index = 0; index < values.length; index++)
			this.histogram.put(values[index], this.histogram.get(values[index]) + weights[index]);

		final ObjectSet<Object2DoubleMap.Entry<IJsonNode>> entrySet = this.histogram.object2DoubleEntrySet();
		double max = 0;
		IJsonNode maxObject = NullNode.getInstance();
		for (final Object2DoubleMap.Entry<IJsonNode> entry : entrySet)
			if (entry.getDoubleValue() > max) {
				max = entry.getDoubleValue();
				maxObject = entry.getKey();
			}
		return maxObject;
	}
}
