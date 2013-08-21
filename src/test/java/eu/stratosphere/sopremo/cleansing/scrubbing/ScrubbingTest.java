package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonUtil;

@RunWith(Parameterized.class)
public class ScrubbingTest {
	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
			new Object[] {
				EvaluationExpression.VALUE,
				EvaluationExpression.VALUE,
				JsonUtil.createObjectNode("stringInsteadOfInteger", "12", "outsideMonthRange", 14,
					"shouldBeNonNull", null) },

			new Object[] {
				new ObjectAccess("stringInsteadOfInteger"),
				new TypeValidationExpression(IntNode.class),
				JsonUtil.createObjectNode("stringInsteadOfInteger", 12, "outsideMonthRange", 14,
					"shouldBeNonNull", null) },

			new Object[] {
				new ObjectAccess("outsideMonthRange"),
				new RangeConstraint(IntNode.valueOf(1), IntNode.valueOf(12)),
				JsonUtil.createObjectNode("stringInsteadOfInteger", "12", "outsideMonthRange", 12,
					"shouldBeNonNull", null) },

			new Object[] {
				new ObjectAccess("shouldBeNonNull"),
				new NonNullConstraint(),
				ERROR });
	}

	private static final IJsonNode ERROR = null;

	private PathSegmentExpression path;

	private List<EvaluationExpression> validationRules;

	private IJsonNode expectedObject;

	@SuppressWarnings("unchecked")
	public ScrubbingTest(PathSegmentExpression path, Object validationRules,
			IJsonNode expectedObject) {
		this.path = path;
		this.validationRules = validationRules instanceof Collection ?
			new ArrayList<EvaluationExpression>((Collection<EvaluationExpression>) validationRules) :
			Arrays.asList((EvaluationExpression) validationRules);
		this.expectedObject = expectedObject;
	}

	@Test @org.junit.Ignore
	public void testMapping() {
		final RuleBasedScrubbing scrubbing = new RuleBasedScrubbing();
		final SopremoTestPlan EqualCloneTestPlan = new SopremoTestPlan(scrubbing);
		for (int index = 0; index < this.validationRules.size(); index++)
			scrubbing.addRule(this.validationRules.get(index), this.path);
		Object[] fields = { "stringInsteadOfInteger", "12", "outsideMonthRange", 14, "shouldBeNonNull", null };

		EqualCloneTestPlan.getInput(0).addObject(fields);
		if (this.expectedObject == ERROR)
			EqualCloneTestPlan.getExpectedOutput(0).setEmpty();
		else
			EqualCloneTestPlan.getExpectedOutput(0).add(this.expectedObject);
		EqualCloneTestPlan.run();
	}
}
