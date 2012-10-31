package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.List;

import eu.stratosphere.sopremo.type.IJsonNode;

public class BlackListRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4224451859875263084L;

	private List<IJsonNode> blacklistedValues;

	@SuppressWarnings("unchecked")
	public BlackListRule(List<? extends IJsonNode> blacklistedValues) {
		this.blacklistedValues = (List<IJsonNode>) blacklistedValues;
	}

	@SuppressWarnings("unchecked")
	public BlackListRule(List<? extends IJsonNode> blacklistedValues, IJsonNode defaultValue) {
		this.blacklistedValues = (List<IJsonNode>) blacklistedValues;
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	protected boolean validate(IJsonNode value, ValidationContext context) {
		return !this.blacklistedValues.contains(value);
	}

}
