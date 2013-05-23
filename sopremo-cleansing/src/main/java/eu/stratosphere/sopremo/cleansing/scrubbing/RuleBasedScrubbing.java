package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.cleansing.fusion.ValueTreeContains;
import eu.stratosphere.sopremo.expressions.ChainedSegmentExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.SopremoUtil;

@InputCardinality(1)
@OutputCardinality(1)
public class RuleBasedScrubbing extends CompositeOperator<RuleBasedScrubbing> {
	private Multimap<PathSegmentExpression, EvaluationExpression> rules = ArrayListMultimap.create();

	public void addRule(EvaluationExpression ruleExpression, PathSegmentExpression target) {
		this.rules.put(target, ruleExpression);
	}

	public void removeRule(EvaluationExpression ruleExpression, PathSegmentExpression target) {
		this.rules.remove(target, ruleExpression);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(eu.stratosphere.sopremo.operator.SopremoModule
	 * , eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		if (this.rules.isEmpty()) {
			// short circuit
			module.getOutput(0).setInput(0, module.getInput(0));
			return;
		}

		Projection normalization = new Projection().
			withResultProjection(this.createResultProjection()).
			withInputs(module.getInput(0));
		Selection filterInvalid = new Selection().
			withCondition(new UnaryExpression(new ValueTreeContains(FilterRecord.Instance), true)).
			withInputs(normalization);
		module.getOutput(0).setInput(0, filterInvalid);
	}

	/**
	 * @return
	 */
	private EvaluationExpression createResultProjection() {
		// no nested rule
		if (this.rules.size() == 1 && this.rules.containsKey(EvaluationExpression.VALUE))
			return new ChainedSegmentExpression(this.rules.values());

		Queue<PathSegmentExpression> uncoveredPaths = new LinkedList<PathSegmentExpression>(this.rules.keySet());

		final ObjectCreation objectCreation = new ObjectCreation();
		objectCreation.addMapping(new ObjectCreation.CopyFields(EvaluationExpression.VALUE));
		while (!uncoveredPaths.isEmpty()) {
			final PathSegmentExpression path = uncoveredPaths.remove();
			this.addToObjectCreation(objectCreation, path, path, new ChainedSegmentExpression(this.rules.get(path)).withTail(path));
		}
		return objectCreation;
	}

	/**
	 * @param objectCreation
	 * @param path
	 * @param chainedSegmentExpression
	 */
	private void addToObjectCreation(ObjectCreation objectCreation, PathSegmentExpression remainingPath,
			PathSegmentExpression completePath,
			PathSegmentExpression chainedSegmentExpression) {

		final String field = ((ObjectAccess) remainingPath).getField();

		for (int index = 0, size = objectCreation.getMappingSize(); index < size; index++) {
			final Mapping<?> mapping = objectCreation.getMapping(index);
			final PathSegmentExpression targetExpression = mapping.getTargetExpression();
			if (targetExpression.equalsThisSeqment(targetExpression)) {
				if (remainingPath.getInputExpression() == EvaluationExpression.VALUE)
					objectCreation.addMapping(new ObjectCreation.FieldAssignment(field, chainedSegmentExpression));
				else
					this.addToObjectCreation((ObjectCreation) mapping.getExpression(),
						(PathSegmentExpression) remainingPath.getInputExpression(), completePath,
						chainedSegmentExpression);
				return;
			}
		}

		if (remainingPath.getInputExpression() == EvaluationExpression.VALUE)
			objectCreation.addMapping(new ObjectCreation.FieldAssignment(field, chainedSegmentExpression));
		else {
			final ObjectCreation subObject = new ObjectCreation();
			PathSegmentExpression processedPath = EvaluationExpression.VALUE;
			for (PathSegmentExpression segment = completePath; remainingPath != segment; segment = (PathSegmentExpression) segment
				.getInputExpression())
				processedPath = segment.cloneSegment().withTail(processedPath);
			subObject.addMapping(new ObjectCreation.CopyFields(processedPath));
			objectCreation.addMapping(new ObjectCreation.FieldAssignment(field, subObject));
			this.addToObjectCreation(subObject, (PathSegmentExpression) remainingPath.getInputExpression(),
				completePath, chainedSegmentExpression);
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.rules.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		RuleBasedScrubbing other = (RuleBasedScrubbing) obj;
		return this.rules.equals(other.rules);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.ElementaryOperator#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		super.appendAsString(appendable);
		SopremoUtil.append(appendable, " with rules: ", this.rules);
	}

	/**
	 * 
	 */
	public void clear() {
		this.rules.clear();
	}
}
