package eu.stratosphere.sopremo.cleansing.duplicatedection;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class Preselection extends AbstractSopremoType implements ISerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6468595574993441809L;

	public abstract boolean shouldProcess(IJsonNode left, IJsonNode right);
}
