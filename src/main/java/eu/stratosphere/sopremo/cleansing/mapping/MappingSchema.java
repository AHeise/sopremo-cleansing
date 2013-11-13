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
import it.unibas.spicy.model.datasource.nodes.SequenceNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javolution.text.TextFormat;
import javolution.text.TypeFormat;
import eu.stratosphere.sopremo.AbstractSopremoType;

public class MappingSchema extends AbstractSopremoType {

	private int size = 0;

	private String label;

	private Map<String, List<String>> groupings = new HashMap<String, List<String>>();

	MappingSchema() {

	}

	public MappingSchema(int size, String label) {
		this.size = size;
		this.label = label;
	}

	public int getSize() {
		return this.size;
	}

	public Map<String, List<String>> getGroupings() {
		return this.groupings;
	}

	public void addKeyToInput(String input, String key) {
		if (this.groupings.containsKey(input)) {
			this.groupings.get(input).add(key);
		} else {
			this.groupings.put(input, new ArrayList<String>());
			addKeyToInput(input, key);
		}
	}

	public INode generateSpicyType() {
		SequenceNode schema = new SequenceNode(this.label);

		INode sourceEntities;

		// source : SequenceNode
		// entities_in0 : SetNode
		// entity_in0 : SequenceNode
		// entities_in1 : SetNode
		// entity_in1 : SequenceNode

		for (int index = 0; index < this.size; index++) {
			final String input = EntityMapping.inputPrefixStr
				+ String.valueOf(index);
			INode tempEntity = new SequenceNode(EntityMapping.entityStr + input);
			sourceEntities = new SetNode(EntityMapping.entitiesStr + input);
			sourceEntities.addChild(tempEntity);
			schema.addChild(sourceEntities);
		}
		schema.setRoot(true);

		// ##### extend schema #####

		for (Entry<String, List<String>> grouping : this.groupings.entrySet()) {
			for (String value : grouping.getValue()) {

				INode sourceAttr;

				INode sourceEntity = schema.getChild(
					EntityMapping.entitiesStr + grouping.getKey())
					.getChild(EntityMapping.entityStr + grouping.getKey());

				if (sourceEntity.getChild(value) == null) {
					sourceAttr = new AttributeNode(value);
					sourceAttr.addChild(EntityMapping.dummy);
					sourceEntity.addChild(sourceAttr);
				}
			}
		}

		return schema;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("MappingSchema [size=");
		TypeFormat.format(this.size, appendable);
		appendable.append(", label=");
		appendable.append(this.label);
		appendable.append(", groupings=");
		TextFormat.getDefault(this.groupings.getClass()).format(this.groupings);
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
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MappingSchema other = (MappingSchema) obj;
		return this.size == other.size && this.label.equals(other.label) && this.groupings.equals(other.groupings);
	}

}
