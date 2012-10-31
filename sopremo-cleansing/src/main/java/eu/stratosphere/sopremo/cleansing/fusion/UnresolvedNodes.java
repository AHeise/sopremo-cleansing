package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.Collection;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class UnresolvedNodes extends ArrayNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5303570716798295492L;

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
