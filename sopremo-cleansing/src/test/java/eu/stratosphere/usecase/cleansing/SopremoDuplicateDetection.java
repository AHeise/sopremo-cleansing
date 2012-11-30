package eu.stratosphere.usecase.cleansing;

import java.io.File;

import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.CsvInputFormat;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.TextNode;

public class SopremoDuplicateDetection {

	public static void main(String[] args) {
		// input
		File fileIn = new File("data/restaurant.csv");
		Source source = new Source(CsvInputFormat.class, "file://" + fileIn.getAbsolutePath());
		source.setParameter(CsvInputFormat.COLUMN_NAMES, new String[] { "id", "name", "address", "city", "phone",
			"type" });

		// block first 2 letters
		EvaluationExpression firstTwoLettersEqual = new ComparativeExpression(new MethodCall("substring",
			createPath("0", "name"), new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(
				IntNode.valueOf(2))), BinaryOperator.EQUAL, new MethodCall("substring",
			createPath("1", "name"), new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(
				IntNode.valueOf(2))));
		EvaluationExpression condition = new AndExpression(firstTwoLettersEqual);
		Join blockJoin = new Join().withInputs(source, source).withJoinCondition(condition);

		// selection(id1!=id2)
		Selection select = new Selection().withInputs(blockJoin).
			withCondition(new ComparativeExpression(createPath("0", "id"), BinaryOperator.LESS, createPath("1", "id")));

		// similarity
		Selection sim = new Selection().withInputs(select).
			withCondition(
				new ComparativeExpression(createPath("0", "name"), BinaryOperator.EQUAL, createPath("1", "name")));

		// selection phone number equality
		EvaluationExpression phone1 = new MethodCall("replaceAll", createPath("0", "phone"),
			new ConstantExpression(new TextNode("\\W")), new ConstantExpression(
				new TextNode("")));
		EvaluationExpression phone2 = new MethodCall("replaceAll", createPath("1", "phone"),
			new ConstantExpression(new TextNode("\\W")), new ConstantExpression(
				new TextNode("")));
		Selection phoneSimilarity = new Selection().withInputs(sim).
			withCondition(new ComparativeExpression(phone1, BinaryOperator.EQUAL, phone2));

		// project id
		Projection proj = new Projection().withInputs(phoneSimilarity).
			withValueTransformation(new ArrayCreation(createPath("0", "id"), createPath("1", "id")));

		// output
		File fileOut = new File("data/restaurant_out.json");
		final SopremoPlan sopremoPlan = new SopremoPlan();
		sopremoPlan.setSinks(new Sink("file://" + fileOut.getAbsolutePath()).withInputs(proj));
		// SopremoUtil.trace();
		sopremoPlan.getContext().getFunctionRegistry().register(DuplicateDetectionFunctions.class);
		new TestPlan(sopremoPlan.assemblePact()).run();
	}

	public static class DuplicateDetectionFunctions {
		public static IJsonNode replaceAll(IJsonNode node, TextNode regex, TextNode replacement) {
			return new TextNode(((NumericNode) node).getValueAsText().replaceAll(regex.getTextValue(),
				replacement.getTextValue()));
		}
	}
}
