package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.type.IntNode;

public class PatternValidationRuleTest extends EqualCloneTest<PatternValidationRule>{

	@Override
	protected PatternValidationRule createDefaultInstance(int index) {
		PatternValidationRule foo = new PatternValidationRule(Pattern.compile(String.valueOf(index))); 
		DefaultValueCorrection bar = new DefaultValueCorrection(IntNode.valueOf(index));
		foo.setValueCorrection(bar);
		return foo;
	}

}
