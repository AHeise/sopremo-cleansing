package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.Iterator;

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class MergeRule extends ConflictResolution {
	/**
	 * 
	 */
	private static final long serialVersionUID = -281898889096008741L;

	/**
	 * The default, stateless instance.
	 */
	public final static MergeRule INSTANCE = new MergeRule();

	@Override
	public void fuse(final IArrayNode values, final double[] weights, final FusionContext context) {
		Iterator<IJsonNode> iterator = values.iterator();
		while (iterator.hasNext()) 
			if(iterator.next().isNull())
				iterator.remove();
	}
}
