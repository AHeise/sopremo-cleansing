package eu.stratosphere.sopremo.cleansing.duplicatedection;

import eu.stratosphere.sopremo.ISopremoType;
import eu.stratosphere.sopremo.type.IJsonNode;

public interface Preselection extends ISopremoType {
	boolean shouldProcess(IJsonNode left, IJsonNode right);
}
