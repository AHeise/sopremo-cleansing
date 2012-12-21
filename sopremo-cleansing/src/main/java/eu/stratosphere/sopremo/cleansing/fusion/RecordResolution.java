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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.ISopremoType;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author Arvid Heise
 */
public abstract class RecordResolution extends ConflictResolution<IObjectNode> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5841402171573265477L;

	private Map<String, ConflictResolution<?>> rules = new HashMap<String, ConflictResolution<?>>();

	private transient IObjectNode fusedRecord = new ObjectNode();

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.fusion.ConflictResolution#fuse(eu.stratosphere.sopremo.type
	 * .IArrayNode)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void fuse(IArrayNode<IObjectNode> objects) {
		this.fusedRecord.clear();

		for (IObjectNode object : objects) {
			for (Entry<String, IJsonNode> field : object) {
				IJsonNode fieldValues = this.fusedRecord.get(field.getKey());
				if (fieldValues.isMissing())
					this.fusedRecord.put(field.getKey(), fieldValues = new ArrayNode<IJsonNode>());
				((IArrayNode<IJsonNode>) fieldValues).add(field.getValue());
			}
		}

		for (Entry<String, ConflictResolution<?>> rule : this.rules.entrySet()) {
			final IJsonNode entry = this.fusedRecord.get(rule.getKey());
			if (!entry.isMissing()) 
				this.fusedRecord.put(rule.getKey(), rule.getValue().evaluate(entry));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.PathSegmentExpression#copyPropertiesFrom(eu.stratosphere.sopremo.ISopremoType
	 * )
	 */
	@Override
	public void copyPropertiesFrom(ISopremoType original) {
		super.copyPropertiesFrom(original);
		for (Entry<String, ConflictResolution<?>> rule : ((RecordResolution) original).rules.entrySet())
			this.rules.put(rule.getKey(), (ConflictResolution<?>) rule.getValue().clone());
	}
}
