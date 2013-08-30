package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@Name(noun="mergeDistinct")
public class MergeDistinctResolution extends ConflictResolution {
	/**
	 * The default, stateless instance.
	 */
	public final static MergeDistinctResolution INSTANCE = new MergeDistinctResolution();

	private transient final Set<IJsonNode> distinctValues = new HashSet<IJsonNode>();

	@Override
	public void fuse(final IArrayNode<IJsonNode> values, final Map<String, CompositeEvidence> weights) {
		this.distinctValues.clear();
		//this.distinctValues.add(NullNode.getInstance());

		Iterator<IJsonNode> iterator = values.iterator();
		while (iterator.hasNext()) {
			IJsonNode element = getValueFromSourceTaggedObject(iterator.next());
			if (this.distinctValues.contains(element))
				iterator.remove();
			else
				this.distinctValues.add(element);
		}
		values.clear();
		values.addAll(this.distinctValues);
	}
}
