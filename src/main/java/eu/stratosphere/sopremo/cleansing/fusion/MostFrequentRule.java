package eu.stratosphere.sopremo.cleansing.fusion;

import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.MissingNode;

public class MostFrequentRule extends ConflictResolution {
	private final Object2DoubleMap<IJsonNode> histogram = new Object2DoubleOpenHashMap<IJsonNode>();

	@Override
	public void fuse(final IArrayNode<IJsonNode> values, final double[] weights) {
		this.histogram.clear();
		for (int index = 0; index < values.size(); index++)
			this.histogram.put(values.get(index), this.histogram.get(values.get(index)) + weights[index]);

		final ObjectSet<Object2DoubleMap.Entry<IJsonNode>> entrySet = this.histogram.object2DoubleEntrySet();
		double max = 0;
		IJsonNode maxObject = MissingNode.getInstance();
		for (final Object2DoubleMap.Entry<IJsonNode> entry : entrySet)
			if (entry.getDoubleValue() > max) {
				max = entry.getDoubleValue();
				maxObject = entry.getKey();
			}
		values.clear();
		if (maxObject != MissingNode.getInstance())
			values.add(maxObject);
	}
}
