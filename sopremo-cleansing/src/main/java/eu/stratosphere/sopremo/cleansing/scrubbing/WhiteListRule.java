package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.List;

import eu.stratosphere.sopremo.type.IJsonNode;

public class WhiteListRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4224451859875263084L;

	private List<IJsonNode> possibleValues;

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends IJsonNode> possibleValues) {
		this.possibleValues = (List<IJsonNode>) possibleValues;
	}

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends IJsonNode> possibleValues, IJsonNode defaultValue) {
		this.possibleValues = (List<IJsonNode>) possibleValues;
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(IJsonNode value, ValidationContext context) {
		return this.possibleValues.contains(value);
	}
}
