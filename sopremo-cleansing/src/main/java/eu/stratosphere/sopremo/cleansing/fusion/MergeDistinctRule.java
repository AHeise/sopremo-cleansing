package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class MergeDistinctRule extends ConflictResolution<IJsonNode> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -281898889096008741L;
	
	private transient final Set<IJsonNode> distinctValues = new HashSet<IJsonNode>();

	@Override
	public void fuse(final IArrayNode<IJsonNode> values) {
		this.distinctValues.clear();
		this.distinctValues.add(NullNode.getInstance());
		
		Iterator<IJsonNode> iterator = values.iterator();
		while (iterator.hasNext()) {
			IJsonNode element = iterator.next();
			if(this.distinctValues.contains(element))
				iterator.remove();
			else this.distinctValues.add(element);
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#createCopy()
	 */
	@Override
	protected EvaluationExpression createCopy() {
		return new MergeDistinctRule();
	}
}
