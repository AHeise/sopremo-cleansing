package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.List;

import eu.stratosphere.sopremo.type.IJsonNode;

public class WhiteListRule extends ValidationRule {
	private final List<IJsonNode> possibleValues;

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends IJsonNode> possibleValues) {
		this.possibleValues = (List<IJsonNode>) possibleValues;
	}

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends IJsonNode> possibleValues,
			IJsonNode defaultValue) {
		this.possibleValues = (List<IJsonNode>) possibleValues;
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	/**
	 * Initializes WhiteListRule.
	 * 
	 */
	WhiteListRule() {
		this.possibleValues = null;
	}

	@Override
	public boolean validate(IJsonNode value) {
		return this.possibleValues.contains(value);
	}

	public List<IJsonNode> getPossibleValues() {
		return this.possibleValues;
	}
}
