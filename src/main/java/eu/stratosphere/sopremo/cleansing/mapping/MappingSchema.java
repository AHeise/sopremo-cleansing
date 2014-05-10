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
package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.AttributeNode;
import it.unibas.spicy.model.datasource.nodes.LeafNode;
import it.unibas.spicy.model.datasource.nodes.MetadataNode;
import it.unibas.spicy.model.datasource.nodes.SequenceNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.datasource.nodes.TupleNode;
import it.unibas.spicy.model.datasource.nodes.UnionNode;
import it.unibas.spicy.model.datasource.operators.INodeVisitor;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javolution.text.TypeFormat;
import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.cleansing.EntityMapping;
import eu.stratosphere.util.reflect.ReflectUtil;

public class MappingSchema extends AbstractSopremoType {

	private int size = 0;

	private String label = "";

	private final Map<Integer, Map<String, Class<? extends INode>>> groupings = new HashMap<Integer, Map<String, Class<? extends INode>>>();

	MappingSchema() {

	}

	public MappingSchema(final int size, final String label) {
		this.size = size;
		this.label = label;
	}

	public int getSize() {
		return this.size;
	}

	public void addKeyToInput(final Integer input, final String key, Class<? extends INode> type) {
		Map<String, Class<? extends INode>> group = this.groupings.get(input);
		if (group == null)
			this.groupings.put(input, group = new LinkedHashMap<String, Class<? extends INode>>());
		if (!group.containsKey(key))
			group.put(key, type);
	}

	public INode generateSpicyType() {
		final SequenceNode schema = new SequenceNode(this.label);

		// source : SequenceNode
		// entities_in0 : SetNode
		// entity_in0 : SequenceNode
		// entities_in1 : SetNode
		// entity_in1 : SequenceNode

		List<INode> sourceNodes = new ArrayList<INode>();
		for (int index = 0; index < this.size; index++) {
			INode sourceEntities = new SetNode(String.valueOf(index));
			schema.addChild(sourceEntities);

			final INode sourceEntity = new SequenceNode("object");
			sourceEntities.addChild(sourceEntity);
			sourceNodes.add(sourceEntity);
		}
		schema.setRoot(true);

		// ##### extend schema #####

		for (final Entry<Integer, Map<String, Class<? extends INode>>> grouping : this.groupings.entrySet())
			for (final Map.Entry<String, Class<? extends INode>> groupingEntry : grouping.getValue().entrySet()) {
				final String value = groupingEntry.getKey();
				final String[] valueSteps = value.split("\\" + EntityMapping.separator);

				INode sourceNode = sourceNodes.get(grouping.getKey());

				for (int i = 0; i < valueSteps.length - 1; i++)
					sourceNode = this.addToSchemaTree(valueSteps[i], sourceNode);

				if (sourceNode.getChild(valueSteps[valueSteps.length - 1]) == null) {
					String step = valueSteps[valueSteps.length - 1];
					INode sourceAttr = ReflectUtil.newInstance(groupingEntry.getValue(), step);
					sourceNode.addChild(sourceAttr);
				}
			}

		addLeafNodes(schema);

		return schema;
	}

	/**
	 * 
	 */
	private void addLeafNodes(INode schema) {
		List<INode> children = schema.getChildren();
		if (children.isEmpty())
			schema.addChild(EntityMapping.dummy);
		else
			for (INode child : children)
				addLeafNodes(child);
	}

	private INode addToSchemaTree(final String value, final INode sourceNode) {
		if (sourceNode.getChild(value) == null) {
			final INode newSetNode = new UnionNode(value);
			sourceNode.addChild(newSetNode);

			final INode newSeequenceNode = new TupleNode(EntityMapping.dummy.getLabel());

			newSetNode.addChild(newSeequenceNode);
			return newSetNode;
		}
		return sourceNode.getChild(value);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		appendable.append("MappingSchema [size=");
		TypeFormat.format(this.size, appendable);
		appendable.append(", label=");
		appendable.append(this.label);
		appendable.append(", groupings=");
		appendable.append(this.groupings.toString());
		// TextFormat.getDefault(this.groupings.getClass()).format(this.groupings);
		appendable.append("]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.groupings.hashCode();
		result = prime * result + this.label.hashCode();
		result = prime * result + this.size;
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final MappingSchema other = (MappingSchema) obj;
		return this.size == other.size && this.label.equals(other.label) && this.groupings.equals(other.groupings);
	}

}
