package eu.stratosphere.sopremo.cleansing.duplicatedection;

import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.type.IJsonNode;

public interface Preselection extends ISerializableSopremoType {
	boolean shouldProcess(IJsonNode left, IJsonNode right);
}
