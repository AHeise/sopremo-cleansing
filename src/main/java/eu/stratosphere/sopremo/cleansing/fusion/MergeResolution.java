package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.Iterator;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class MergeResolution extends ConflictResolution {
	/**
	 * The default, stateless instance.
	 */
	public final static MergeResolution INSTANCE = new MergeResolution();

	@Override
	public void fuse(final IArrayNode<IJsonNode> values, final double[] weights) {
		Iterator<IJsonNode> iterator = values.iterator();
		while (iterator.hasNext())
			if (iterator.next() == NullNode.getInstance())
				iterator.remove();
	}
}
