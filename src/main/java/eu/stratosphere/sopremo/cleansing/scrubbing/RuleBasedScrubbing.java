package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.cleansing.ScrubbingSpecificChainedSegmentExpression;
import eu.stratosphere.sopremo.cleansing.fusion.ValueTreeContains;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Internal;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.SopremoUtil;

@InputCardinality(1)
@OutputCardinality(1)
@DefaultSerializer(value = RuleBasedScrubbing.RuleBasedScrubbingSerializer.class)
@Internal
public class RuleBasedScrubbing extends CompositeOperator<RuleBasedScrubbing> {
	public static class RuleBasedScrubbingSerializer extends
			Serializer<RuleBasedScrubbing> {
		FieldSerializer<RuleBasedScrubbing> fieldSerializer;

		public RuleBasedScrubbingSerializer(Kryo kryo,
				Class<RuleBasedScrubbing> type) {
			fieldSerializer = new FieldSerializer<RuleBasedScrubbing>(kryo,
					type);
		}

		@Override
		public void write(Kryo kryo,
				com.esotericsoftware.kryo.io.Output output,
				RuleBasedScrubbing object) {
			fieldSerializer.write(kryo, output, object);
			Map<PathSegmentExpression, Collection<EvaluationExpression>> backingMapCopy = new HashMap<PathSegmentExpression, Collection<EvaluationExpression>>();
			for (Entry<PathSegmentExpression, Collection<EvaluationExpression>> entry : object.rules
					.asMap().entrySet()) {
				Collection<EvaluationExpression> tempList = new ArrayList<EvaluationExpression>();
				tempList.addAll(entry.getValue());
				backingMapCopy.put(entry.getKey(), tempList);
			}

			kryo.writeObject(output, backingMapCopy, new MapSerializer());
		}

		@Override
		public RuleBasedScrubbing read(Kryo kryo, Input input,
				Class<RuleBasedScrubbing> type) {
			RuleBasedScrubbing object = fieldSerializer.read(kryo, input, type);
			MapSerializer mapSerializer = new MapSerializer();
			mapSerializer.setKeyClass(PathSegmentExpression.class, null);
			mapSerializer.setValueClass(Collection.class, null);
			@SuppressWarnings("unchecked")
			Map<PathSegmentExpression, Collection<EvaluationExpression>> backingMapCopy = kryo
					.readObject(input, HashMap.class, mapSerializer);
			for (Entry<PathSegmentExpression, Collection<EvaluationExpression>> entry : backingMapCopy
					.entrySet()) {
				object.rules.putAll(entry.getKey(), entry.getValue());
			}

			return object;
		}

		@Override
		public RuleBasedScrubbing copy(Kryo kryo, RuleBasedScrubbing original) {
			RuleBasedScrubbing copy = this.fieldSerializer.copy(kryo, original);
			for (Entry<PathSegmentExpression, EvaluationExpression> rule : original.rules
					.entries()) {
				copy.addRule((EvaluationExpression) rule.getValue().copy(),
						(PathSegmentExpression) rule.getKey().copy());
			}
			return copy;
		}
	}

	public RuleBasedScrubbing() {
		@SuppressWarnings("unused")
		int i = 0;
	}

	private Multimap<PathSegmentExpression, EvaluationExpression> rules = ArrayListMultimap
			.create();

	public void addRule(EvaluationExpression ruleExpression,
			PathSegmentExpression target) {
		this.rules.put(target, ruleExpression);
	}

	public void removeRule(EvaluationExpression ruleExpression,
			PathSegmentExpression target) {
		this.rules.remove(target, ruleExpression);
	}

	@Override
	public Operator<RuleBasedScrubbing> clone() {
		return super.clone();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(
	 * eu.stratosphere.sopremo.operator.SopremoModule ,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module) {
		if (this.rules.isEmpty()) {
			// short circuit
			module.getOutput(0).setInput(0, module.getInput(0));
			return;
		}

		Projection normalization = new Projection().withResultProjection(
				this.createResultProjection()).withInputs(module.getInput(0));
		Selection filterInvalid = new Selection().withCondition(
				new UnaryExpression(
						new ValueTreeContains(FilterRecord.Instance), true))
				.withInputs(normalization);
		module.getOutput(0).setInput(0, filterInvalid);
	}

	/**
	 * @return
	 */
	private EvaluationExpression createResultProjection() {
		// no nested rule
		if (this.rules.size() == 1
				&& this.rules.containsKey(EvaluationExpression.VALUE))
			return new ScrubbingSpecificChainedSegmentExpression(
					this.rules.values());

		Queue<PathSegmentExpression> uncoveredPaths = new LinkedList<PathSegmentExpression>(
				this.rules.keySet());

		final ObjectCreation objectCreation = new ObjectCreation();
		objectCreation.addMapping(new ObjectCreation.CopyFields(
				EvaluationExpression.VALUE));
		while (!uncoveredPaths.isEmpty()) {
			final PathSegmentExpression path = uncoveredPaths.remove();
			this.addToObjectCreation(
					objectCreation,
					path,
					path,
					new ScrubbingSpecificChainedSegmentExpression(this.rules
							.get(path)).withTail(path));
		}
		return objectCreation;
	}

	/**
	 * @param objectCreation
	 * @param path
	 * @param chainedSegmentExpression
	 */
	private void addToObjectCreation(ObjectCreation objectCreation,
			PathSegmentExpression remainingPath,
			PathSegmentExpression completePath,
			PathSegmentExpression chainedSegmentExpression) {

		final String field = ((ObjectAccess) remainingPath).getField();

		for (int index = 0, size = objectCreation.getMappingSize(); index < size; index++) {
			final Mapping<?> mapping = objectCreation.getMapping(index);
			final PathSegmentExpression targetExpression = mapping
					.getTargetExpression();
			// if (targetExpression.equalsThisSeqment(targetExpression)) {
			
			// TODO add case for ObjectAccess to allow complex objects
			// Testcase: ScrubbingComplexTest.java
			if (remainingPath.getInputExpression() == EvaluationExpression.VALUE)
				objectCreation.addMapping(new ObjectCreation.FieldAssignment(
						field, chainedSegmentExpression));
			else
				this.addToObjectCreation((ObjectCreation) mapping
						.getExpression(), (PathSegmentExpression) remainingPath
						.getInputExpression(), completePath,
						chainedSegmentExpression);
			return;
			// }
		}

		if (remainingPath.getInputExpression() == EvaluationExpression.VALUE)
			objectCreation.addMapping(new ObjectCreation.FieldAssignment(field,
					chainedSegmentExpression));
		else {
			final ObjectCreation subObject = new ObjectCreation();
			PathSegmentExpression processedPath = EvaluationExpression.VALUE;
			for (PathSegmentExpression segment = completePath; remainingPath != segment; segment = (PathSegmentExpression) segment
					.getInputExpression())
				processedPath = segment.cloneSegment().withTail(processedPath);
			subObject.addMapping(new ObjectCreation.CopyFields(processedPath));
			objectCreation.addMapping(new ObjectCreation.FieldAssignment(field,
					subObject));
			this.addToObjectCreation(subObject,
					(PathSegmentExpression) remainingPath.getInputExpression(),
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

		// for (Entry<PathSegmentExpression, EvaluationExpression> entry :
		// this.rules
		// .entries()) {
		// if (!other.rules.containsEntry(entry.getKey(), entry.getValue())) {
		// return false;
		// }
		// }
		// for (Entry<PathSegmentExpression, EvaluationExpression> entry :
		// other.rules
		// .entries()) {
		// if (!this.rules.containsEntry(entry.getKey(), entry.getValue())) {
		// return false;
		// }
		// }
		// return true;
		return this.rules.equals(other.rules);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.ElementaryOperator#appendAsString(java
	 * .lang.Appendable)
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
