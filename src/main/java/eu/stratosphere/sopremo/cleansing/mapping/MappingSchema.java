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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MappingSchema {

	private int size = 0;
	private String label;
	private Map<String, Set<String>> groupings = new HashMap<String, Set<String>>();

	MappingSchema(){
		
	}
	
	public MappingSchema(int size, String label) {
		this.size = size;
		this.label = label;
	}

	public int getSize() {
		return this.size;
	}

	public Map<String, Set<String>> getGroupings() {
		return this.groupings;
	}
	
	public void addKeyToInput(String input, String key){
		if(this.groupings.containsKey(input)){
			this.groupings.get(input).add(key);
		}else{
			this.groupings.put(input, new HashSet<String>());
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

		for (int index = 0; index < size; index++) {
			final String input = EntityMapping.inputPrefixStr
					+ String.valueOf(index);
			INode tempEntity = new SequenceNode(EntityMapping.entityStr + input);
			sourceEntities = new SetNode(EntityMapping.entitiesStr + input);
			sourceEntities.addChild(tempEntity);
			schema.addChild(sourceEntities);
		}
		schema.setRoot(true);

		// ##### extend schema #####

		for (Entry<String, Set<String>> grouping : this.groupings.entrySet()) {
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
}
