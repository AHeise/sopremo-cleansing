package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * This class provides the functionality to specify certain characters that are
 * not allowed in the value of a record-field. The following example shows the
 * usage of this rule in a meteor-script:
 * 
 * <code><pre>
 * ...
 * $persons_scrubbed = scrub $persons_sample with rules {
 *	...
 *	name: illegalCharacters("%", "$", "!", "[", "]"),
 *	...
 * };
 * ...
 * </pre></code>
 * 
 * @author Arvid Heise, Tommy Neubert, Fabian Tschirschnitz
 */
public class IllegalCharacterRule extends ValidationRule {

	private TextNode illegalCharacters;

	public IllegalCharacterRule(TextNode illegalCharacters) {
		this.illegalCharacters = illegalCharacters;
	}

	public IllegalCharacterRule() {
		this.illegalCharacters = null;
	}

	@Override
	public boolean validate(IJsonNode node) {
		for (int i = 0; i < node.toString().length(); i++) {
			for (Character c : this.illegalCharacters.toString().toCharArray()) {
				if (c.charValue() == node.toString().charAt(i))
					return false;
			}
		}
		return true;
	}

	public char[] getIllegalCharacters() {
		return this.illegalCharacters.toString().toCharArray();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime
				* result
				+ ((illegalCharacters == null) ? 0 : illegalCharacters
						.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		IllegalCharacterRule other = (IllegalCharacterRule) obj;
		if (illegalCharacters == null) {
			if (other.illegalCharacters != null)
				return false;
		} else if (!illegalCharacters.equals(other.illegalCharacters))
			return false;
		return true;
	}
}
