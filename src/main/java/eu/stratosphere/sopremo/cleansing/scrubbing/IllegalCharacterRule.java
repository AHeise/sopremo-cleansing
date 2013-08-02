package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

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
}
