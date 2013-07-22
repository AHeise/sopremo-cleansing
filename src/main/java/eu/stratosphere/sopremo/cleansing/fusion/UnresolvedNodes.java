package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.Collection;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class UnresolvedNodes extends ArrayNode<IJsonNode> {

	public UnresolvedNodes() {
		super();
	}

	public UnresolvedNodes(Collection<? extends IJsonNode> nodes) {
		super(nodes);
	}

	public UnresolvedNodes(IJsonNode... nodes) {
		super(nodes);
	}

}
