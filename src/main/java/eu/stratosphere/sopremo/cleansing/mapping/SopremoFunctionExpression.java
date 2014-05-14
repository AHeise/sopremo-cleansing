package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.exceptions.ExpressionSyntaxException;
import it.unibas.spicy.model.expressions.Expression;

import java.lang.reflect.Field;

import org.nfunk.jep.JEP;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class SopremoFunctionExpression extends Expression {

	public SopremoFunctionExpression(final String expression) throws ExpressionSyntaxException {
		super(expression);
		throw new UnsupportedOperationException("calling constructor with expression string is forbidden on " +
			SopremoFunctionExpression.class);
	}

	public SopremoFunctionExpression(final EvaluationExpression expr) {
		super("sum(1,1)");

		Field topNodeField = null;

		try {
			topNodeField = JEP.class.
				getDeclaredField("topNode");
		} catch (final NoSuchFieldException e) {
			e.printStackTrace();
		} catch (final SecurityException e) {
			e.printStackTrace();
		}

		topNodeField.setAccessible(true);
		final FunctionNode fnNode = new FunctionNode(0, expr);

		try {
			topNodeField.set(this.getJepExpression(), fnNode);
		} catch (final IllegalArgumentException e) {
			e.printStackTrace();
		} catch (final IllegalAccessException e) {
			e.printStackTrace();
		}

	}

}
