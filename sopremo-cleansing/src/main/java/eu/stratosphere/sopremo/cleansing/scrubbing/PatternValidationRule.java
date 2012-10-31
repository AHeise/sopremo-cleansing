package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

public class PatternValidationRule extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = -414237332065402567L;

	private final Pattern pattern;

	public PatternValidationRule(final Pattern pattern) {
		this.pattern = pattern;
	}

	@Override
	protected boolean validate(final IJsonNode node, final ValidationContext context) {
		return this.pattern.matcher(TypeCoercer.INSTANCE.coerce(node, TextNode.class).getTextValue())
			.matches();
	}
}
