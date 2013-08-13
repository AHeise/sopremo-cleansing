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

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.expressions.ChainedSegmentExpression;
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
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author Arvid Heise
 */
@InputCardinality(1)
@OutputCardinality(1)
@DefaultSerializer(value = RuleBasedFusion.RuleBasedFusionSerializer.class)
public class RuleBasedFusion extends CompositeOperator<RuleBasedFusion> {
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((resolutions == null) ? 0 : resolutions.hashCode());
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
		RuleBasedFusion other = (RuleBasedFusion) obj;
		if (resolutions == null) {
			if (other.resolutions != null)
				return false;
		} else if (!resolutions.equals(other.resolutions))
			return false;
		return true;
	}

	public static class RuleBasedFusionSerializer extends Serializer<RuleBasedFusion> {
		FieldSerializer<RuleBasedFusion> fieldSerializer;

		public RuleBasedFusionSerializer(Kryo kryo, Class<RuleBasedFusion> type) {
			fieldSerializer = new FieldSerializer<RuleBasedFusion>(kryo, type);
		}

		@Override
		public void write(Kryo kryo, com.esotericsoftware.kryo.io.Output output, RuleBasedFusion object) {
			fieldSerializer.write(kryo, output, object);
			Map<PathSegmentExpression, Collection<EvaluationExpression>> backingMapCopy = new HashMap<PathSegmentExpression, Collection<EvaluationExpression>>();
			for (Entry<PathSegmentExpression, Collection<EvaluationExpression>> entry : object.resolutions.asMap().entrySet()) {
				Collection<EvaluationExpression> tempList = new ArrayList<EvaluationExpression>();
				tempList.addAll(entry.getValue());
				backingMapCopy.put(entry.getKey(), tempList);
			}

			kryo.writeObject(output, backingMapCopy, new MapSerializer());
		}

		@Override
		public RuleBasedFusion read(Kryo kryo, Input input, Class<RuleBasedFusion> type) {
			RuleBasedFusion object = fieldSerializer.read(kryo, input, type);
			MapSerializer mapSerializer = new MapSerializer();
			mapSerializer.setKeyClass(PathSegmentExpression.class, null);
			mapSerializer.setValueClass(Collection.class, null);
			@SuppressWarnings("unchecked")
			Map<PathSegmentExpression, Collection<EvaluationExpression>> backingMapCopy = kryo.readObject(input, HashMap.class, mapSerializer);
			for (Entry<PathSegmentExpression, Collection<EvaluationExpression>> entry : backingMapCopy.entrySet()) {

				object.resolutions.putAll(entry.getKey(), entry.getValue());
			}

			return object;
		}

		@Override
		public RuleBasedFusion copy(Kryo kryo, RuleBasedFusion original) {
			RuleBasedFusion copy = this.fieldSerializer.copy(kryo, original);
			for (Entry<PathSegmentExpression, EvaluationExpression> rule : original.resolutions.entries()) {
				copy.addResolution((EvaluationExpression) rule.getValue().copy(), (PathSegmentExpression) rule.getKey().copy());
			}
			return copy;
		}
	}

	private Multimap<PathSegmentExpression, EvaluationExpression> resolutions = ArrayListMultimap.create();

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
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		if (this.resolutions.isEmpty()) {
			// short circuit
			module.getOutput(0).setInput(0, module.getInput(0));
			return;
		}

		FusionProjection preProceccingProjection = new FusionProjection().withInputs(module.getInput(0));
		Projection normalization = new Projection().withResultProjection(this.createResultProjection()).withInputs(preProceccingProjection);
		Selection filterInvalid = new Selection().withCondition(new UnaryExpression(new ValueTreeContains(FilterRecord.Instance), true)).withInputs(
				normalization);
		module.getOutput(0).setInput(0, filterInvalid);
	}

	private EvaluationExpression createResultProjection() {
		// no nested rule
		if (this.resolutions.size() == 1 && this.resolutions.containsKey(EvaluationExpression.VALUE))
			return new ChainedSegmentExpression(this.resolutions.values());

		Queue<PathSegmentExpression> uncoveredPaths = new LinkedList<PathSegmentExpression>(this.resolutions.keySet());

		final ObjectCreation objectCreation = new ObjectCreation();
		// objectCreation.addMapping(new
		// ObjectCreation.CopyFields(EvaluationExpression.VALUE));
		while (!uncoveredPaths.isEmpty()) {
			final PathSegmentExpression path = uncoveredPaths.remove();
			this.addToObjectCreation(objectCreation, path, path, new ChainedSegmentExpression(this.resolutions.get(path)).withTail(path));
		}
		return objectCreation;
	}

	private void addToObjectCreation(ObjectCreation objectCreation, PathSegmentExpression remainingPath, PathSegmentExpression completePath,
			PathSegmentExpression chainedSegmentExpression) {

		final String field = ((ObjectAccess) remainingPath).getField();

		for (int index = 0, size = objectCreation.getMappingSize(); index < size; index++) {
			final Mapping<?> mapping = objectCreation.getMapping(index);
			final PathSegmentExpression targetExpression = mapping.getTargetExpression();
			if (targetExpression.equalsThisSeqment(targetExpression)) {
				if (remainingPath.getInputExpression() == EvaluationExpression.VALUE)
					objectCreation.addMapping(new ObjectCreation.FieldAssignment(field, chainedSegmentExpression));
				else
					this.addToObjectCreation((ObjectCreation) mapping.getExpression(), (PathSegmentExpression) remainingPath.getInputExpression(),
							completePath, chainedSegmentExpression);
				return;
			}
		}

		if (remainingPath.getInputExpression() == EvaluationExpression.VALUE)
			objectCreation.addMapping(new ObjectCreation.FieldAssignment(field, chainedSegmentExpression));
		else {
			final ObjectCreation subObject = new ObjectCreation();
			PathSegmentExpression processedPath = EvaluationExpression.VALUE;
			for (PathSegmentExpression segment = completePath; remainingPath != segment; segment = (PathSegmentExpression) segment.getInputExpression())
				processedPath = segment.cloneSegment().withTail(processedPath);
			subObject.addMapping(new ObjectCreation.CopyFields(processedPath));
			objectCreation.addMapping(new ObjectCreation.FieldAssignment(field, subObject));
			this.addToObjectCreation(subObject, (PathSegmentExpression) remainingPath.getInputExpression(), completePath, chainedSegmentExpression);
		}

	}

	public void addResolution(EvaluationExpression ruleExpression, PathSegmentExpression target) {
		this.resolutions.put(target, ruleExpression);
	}

	public void removeResolution(EvaluationExpression ruleExpression, PathSegmentExpression target) {
		this.resolutions.remove(target, ruleExpression);
	}

	@InputCardinality(1)
	public static class FusionProjection extends ElementaryOperator<FusionProjection> {
		
		public PactModule asPactModule(EvaluationContext context, SopremoRecordLayout layout) {
			return super.asPactModule(new FusionContext(context), layout);
		}

		public static class ProjectionStub extends SopremoMap {
			@SuppressWarnings("unchecked")
			@Override
			protected void map(final IJsonNode values, final JsonCollector out) {
				IObjectNode fusedRecord = new ObjectNode();

				for (IJsonNode value : (IArrayNode<IJsonNode>) values) {
					IObjectNode object = (IObjectNode) value;
					for (Entry<String, IJsonNode> field : object) {
						IJsonNode fieldValues = fusedRecord.get(field.getKey());
						if (fieldValues == MissingNode.getInstance())
							fusedRecord.put(field.getKey(), fieldValues = new ArrayNode<IJsonNode>());
						((IArrayNode<IJsonNode>) fieldValues).add(field.getValue());
					}
				}
				out.collect(fusedRecord);
			}
		}
	}

	public void clear() {
		this.resolutions.clear();
	}
}
