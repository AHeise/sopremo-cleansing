package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

public class TypeValidationExpression extends ValidationRule {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5475336828366519516L;

	private final Class<? extends IJsonNode> type;

	public TypeValidationExpression(final Class<? extends IJsonNode> type) {
		this.type = type;
	}

	@Override
	protected IJsonNode fix(final IJsonNode value, final IJsonNode target, final ValidationContext context) {
		try {
			if (value.isTextual())
				return LenientParser.INSTANCE.parse((TextNode) value, this.type,
					LenientParser.ELIMINATE_NOISE);
			return TypeCoercer.INSTANCE.coerce(value, this.type);
		} catch (final Exception e) {
			return super.fix(value, context);
		}
	}

	@Override
	protected boolean validate(final IJsonNode value, final ValidationContext context) {
		return this.type.isInstance(value);
	}

	@Override
	public String toString() {
		return "TypeValidationExpression [type=" + this.type + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.type.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		TypeValidationExpression other = (TypeValidationExpression) obj;
		return this.type.equals(other.type);
	}

}
