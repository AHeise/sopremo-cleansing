package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

public class PatternValidationRule extends ValidationRule {
	private final Pattern pattern;

	public PatternValidationRule(final Pattern pattern) {
		this.pattern = pattern;
	}

	/**
	 * Initializes PatternValidationRule.
	 */
	PatternValidationRule() {
		this.pattern = null;
	}

	private transient NodeCache nodeCache = new NodeCache();

	@Override
	protected boolean validate(final IJsonNode node) {
		return this.pattern.matcher(TypeCoercer.INSTANCE.coerce(node, this.nodeCache, TextNode.class)).matches();
	}
}
