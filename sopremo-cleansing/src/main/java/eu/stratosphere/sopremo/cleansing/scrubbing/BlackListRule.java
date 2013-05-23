package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.List;

import eu.stratosphere.sopremo.type.IJsonNode;

public class BlackListRule extends ValidationRule {
	private final List<IJsonNode> blacklistedValues;

	@SuppressWarnings("unchecked")
	public BlackListRule(List<? extends IJsonNode> blacklistedValues) {
		this.blacklistedValues = (List<IJsonNode>) blacklistedValues;
	}

	@SuppressWarnings("unchecked")
	public BlackListRule(List<? extends IJsonNode> blacklistedValues, IJsonNode defaultValue) {
		this.blacklistedValues = (List<IJsonNode>) blacklistedValues;
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}
	
	BlackListRule() {
		this.blacklistedValues = null;
	}

	@Override
	protected boolean validate(IJsonNode value) {
		return !this.blacklistedValues.contains(value);
	}

}
