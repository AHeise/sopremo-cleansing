/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.cleansing.fusion;

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

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.cleansing.FusionSpecificChainedSegmentExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.GenericSopremoMap;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
@InputCardinality(1)
@OutputCardinality(1)
@DefaultSerializer(value = ResolutionBasedFusion.RuleBasedFusionSerializer.class)
public class ResolutionBasedFusion extends CompositeOperator<ResolutionBasedFusion> {
	public static class RuleBasedFusionSerializer extends
			Serializer<ResolutionBasedFusion> {
		FieldSerializer<ResolutionBasedFusion> fieldSerializer;

		public RuleBasedFusionSerializer(final Kryo kryo, final Class<ResolutionBasedFusion> type) {
			this.fieldSerializer = new FieldSerializer<ResolutionBasedFusion>(kryo, type);
		}

		@Override
		public void write(final Kryo kryo,
				final com.esotericsoftware.kryo.io.Output output,
				final ResolutionBasedFusion object) {
			this.fieldSerializer.write(kryo, output, object);
			final Map<PathSegmentExpression, Collection<EvaluationExpression>> backingMapCopy =
				new HashMap<PathSegmentExpression, Collection<EvaluationExpression>>();
			for (final Entry<PathSegmentExpression, Collection<EvaluationExpression>> entry : object.resolutions
				.asMap().entrySet()) {
				final Collection<EvaluationExpression> tempList = new ArrayList<EvaluationExpression>();
				tempList.addAll(entry.getValue());
				backingMapCopy.put(entry.getKey(), tempList);
			}

			kryo.writeObject(output, backingMapCopy, new MapSerializer());
		}

		@Override
		public ResolutionBasedFusion read(final Kryo kryo, final Input input,
				final Class<ResolutionBasedFusion> type) {
			final ResolutionBasedFusion object = this.fieldSerializer.read(kryo, input, type);
			final MapSerializer mapSerializer = new MapSerializer();
			mapSerializer.setKeyClass(PathSegmentExpression.class, null);
			mapSerializer.setValueClass(Collection.class, null);
			@SuppressWarnings("unchecked")
			final Map<PathSegmentExpression, Collection<EvaluationExpression>> backingMapCopy = kryo
				.readObject(input, HashMap.class, mapSerializer);
			for (final Entry<PathSegmentExpression, Collection<EvaluationExpression>> entry : backingMapCopy
				.entrySet())
				object.resolutions.putAll(entry.getKey(), entry.getValue());

			return object;
		}

		@Override
		public ResolutionBasedFusion copy(final Kryo kryo, final ResolutionBasedFusion original) {
			final ResolutionBasedFusion copy = this.fieldSerializer.copy(kryo, original);
			for (final Entry<PathSegmentExpression, EvaluationExpression> rule : original.resolutions
				.entries())
				copy.addResolution((EvaluationExpression) rule.getValue()
					.copy(), (PathSegmentExpression) rule.getKey().copy());
			return copy;
		}
	}

	private final Multimap<PathSegmentExpression, EvaluationExpression> resolutions = ArrayListMultimap
		.create();

	private Map<String, CompositeEvidence> weights;

	// @Override
	// public void addImplementation(SopremoModule module, EvaluationContext
	// context) {
	// JsonStream pipeline = module.getInput(0);
	//
	// // wrap in array
	// // pipeline = new
	// // Projection().withInputs(pipeline).withResultProjection(new
	// // UnionObjects());
	//
	// pipeline = new
	// Projection().withInputs(pipeline).withResultProjection(this.resolutions);
	//
	// // unwrap in array
	// // pipeline = new
	// // Projection().withInputs(pipeline).withResultProjection(new arrayUn);
	//
	// pipeline = new
	// Selection().withInputs(pipeline).withCondition(UnaryExpression.not(new
	// ValueTreeContains(FilterRecord.Instance)));
	//
	// module.getOutput(0).setInput(0, pipeline);
	// }

	@Override
	public void addImplementation(final SopremoModule module,
			final EvaluationContext context) {
		if (this.resolutions.isEmpty()) {
			// short circuit
			module.getOutput(0).setInput(0, module.getInput(0));
			return;
		}

		context.putParameter("weights", this.weights);
		final FusionPreprocessingProjection preProceccingProjection = new FusionPreprocessingProjection()
			.withInputs(module.getInput(0));
		final Projection normalization = new Projection().withResultProjection(
			this.createResultProjection()).withInputs(
			preProceccingProjection);
		final Selection filterInvalid = new Selection().withCondition(
			new UnaryExpression(
				new ValueTreeContains(FilterRecord.Instance), true))
			.withInputs(normalization);
		final FusionMergeProjection mergeProjection = new FusionMergeProjection()
			.withInputs(filterInvalid);
		module.getOutput(0).setInput(0, mergeProjection);
	}

	private EvaluationExpression createResultProjection() {
		// no nested rule
		if (this.resolutions.size() == 1
			&& this.resolutions.containsKey(EvaluationExpression.VALUE))
			return new FusionSpecificChainedSegmentExpression(
				this.resolutions.values());

		final Queue<PathSegmentExpression> uncoveredPaths = new LinkedList<PathSegmentExpression>(
			this.resolutions.keySet());

		final ObjectCreation objectCreation = new ObjectCreation();
		// objectCreation.addMapping(new
		// ObjectCreation.CopyFields(EvaluationExpression.VALUE));
		while (!uncoveredPaths.isEmpty()) {
			final PathSegmentExpression path = uncoveredPaths.remove();
			this.addToObjectCreation(
				objectCreation,
				path,
				path,
				new FusionSpecificChainedSegmentExpression(this.resolutions
					.get(path)).withTail(path));
		}
		return objectCreation;
	}

	private void addToObjectCreation(final ObjectCreation objectCreation,
			final PathSegmentExpression remainingPath,
			final PathSegmentExpression completePath,
			final PathSegmentExpression chainedSegmentExpression) {

		final String field = ((ObjectAccess) remainingPath).getField();

		for (int index = 0, size = objectCreation.getMappingSize(); index < size; index++) {
			final Mapping<?> mapping = objectCreation.getMapping(index);
			final PathSegmentExpression targetExpression = mapping
				.getTargetExpression();
			if (targetExpression.equalsThisSeqment(targetExpression)) {
				if (remainingPath.getInputExpression() == EvaluationExpression.VALUE)
					objectCreation
						.addMapping(new ObjectCreation.FieldAssignment(
							field, chainedSegmentExpression));
				else
					this.addToObjectCreation((ObjectCreation) mapping
						.getExpression(),
						(PathSegmentExpression) remainingPath
							.getInputExpression(), completePath,
						chainedSegmentExpression);
				return;
			}
		}

		if (remainingPath.getInputExpression() == EvaluationExpression.VALUE)
			objectCreation.addMapping(new ObjectCreation.FieldAssignment(field,
				chainedSegmentExpression));
		else {
			final ObjectCreation subObject = new ObjectCreation();
			PathSegmentExpression processedPath = EvaluationExpression.VALUE;
			for (PathSegmentExpression segment = completePath; remainingPath != segment; segment =
				(PathSegmentExpression) segment
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

	public void addResolution(final EvaluationExpression ruleExpression,
			final PathSegmentExpression target) {
		this.resolutions.put(target, ruleExpression);
	}

	public void removeResolution(final EvaluationExpression ruleExpression,
			final PathSegmentExpression target) {
		this.resolutions.remove(target, ruleExpression);
	}

	@InputCardinality(1)
	public static class FusionPreprocessingProjection extends ElementaryOperator<FusionPreprocessingProjection> {

		public static class ProjectionStub extends GenericSopremoMap<IArrayNode<IObjectNode>, IObjectNode> {
			@SuppressWarnings("unchecked")
			@Override
			protected void map(final IArrayNode<IObjectNode> values, final JsonCollector<IObjectNode> out) {
				final IObjectNode fusedRecord = new ObjectNode();
				// TODO introduce source tagged nodes
				for (final IObjectNode object : values) {
					for (final Entry<String, IJsonNode> field : object) {
						final TextNode source = (TextNode) object.get("_source");
						IJsonNode fieldValues = fusedRecord.get(field.getKey());
						if (fieldValues == MissingNode.getInstance())
							fusedRecord.put(field.getKey(),
								fieldValues = new ArrayNode<IJsonNode>());
						final ObjectNode sourceTaggedNode = new ObjectNode();
						sourceTaggedNode.put("_source", source);
						sourceTaggedNode.put("_value", field.getValue());
						((IArrayNode<IJsonNode>) fieldValues).add(sourceTaggedNode);
					}
				}
				out.collect(fusedRecord);
			}
		}
	}

	@InputCardinality(1)
	public static class FusionMergeProjection extends ElementaryOperator<FusionMergeProjection> {

		public static class ProjectionStub extends GenericSopremoMap<IObjectNode, IObjectNode> {
			private final transient IObjectNode mergedObject = new ObjectNode();

			@SuppressWarnings("unchecked")
			@Override
			protected void map(final IObjectNode object, final JsonCollector<IObjectNode> out) {
				this.mergedObject.clear();
				for (final Entry<String, IJsonNode> field : object) {
					IJsonNode mergedValue;
					if (field.getValue() instanceof IArrayNode
						&& ((IArrayNode<IJsonNode>) field.getValue())
							.size() == 1)
						mergedValue = ((IArrayNode<IJsonNode>) field.getValue())
							.get(0);
					else
						mergedValue = field.getValue();
					this.mergedObject.put(field.getKey(), mergedValue);
				}
				out.collect(this.mergedObject);
			}
		}
	}

	public void clear() {
		this.resolutions.clear();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
			+ (this.resolutions == null ? 0 : this.resolutions.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final ResolutionBasedFusion other = (ResolutionBasedFusion) obj;
		if (this.resolutions == null) {
			if (other.resolutions != null)
				return false;
		} else if (!this.resolutions.equals(other.resolutions))
			return false;
		return true;
	}

	public void setWeights(final Map<String, CompositeEvidence> weights) {
		this.weights = weights;
	}

}
