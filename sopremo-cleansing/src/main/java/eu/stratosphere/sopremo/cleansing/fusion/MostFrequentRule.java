package eu.stratosphere.sopremo.cleansing.fusion;

import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class MostFrequentRule extends ConflictResolution<IJsonNode> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8375051813752883648L;

	private transient final Object2DoubleMap<IJsonNode> histogram = new Object2DoubleOpenHashMap<IJsonNode>();

	@Override
	public void fuse(final IArrayNode<IJsonNode> values) {
		this.histogram.clear();
		final double[] weights = getWeights();
		for (int index = 0; index < values.size(); index++) {
			final IJsonNode value = values.get(index);
			this.histogram.put(value, this.histogram.get(value) + weights[index]);
		}

		final ObjectSet<Object2DoubleMap.Entry<IJsonNode>> entrySet = this.histogram.object2DoubleEntrySet();
		double max = 0;
		values.clear();
		for (final Object2DoubleMap.Entry<IJsonNode> entry : entrySet) {
			final double weightedCount = entry.getDoubleValue();
			if (weightedCount > max) {
				max = weightedCount;
				values.clear();
				values.add(entry.getKey());
			} else if (weightedCount == max)
				values.add(entry.getKey());
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#createCopy()
	 */
	@Override
	protected EvaluationExpression createCopy() {
		return new MostFrequentRule();
	}
}
