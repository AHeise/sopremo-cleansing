package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;

public class NonNullRule extends ValidationRule {

	public NonNullRule() {
	}

	public NonNullRule(IJsonNode defaultValue) {
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	public boolean validate(IJsonNode value) {
		return value != NullNode.getInstance() && value != MissingNode.getInstance();
	}
}
