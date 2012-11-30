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

import java.util.Map.Entry;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import eu.stratosphere.sopremo.ISopremoType;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author Arvid Heise
 */
public abstract class RecordResolution extends ConflictResolution {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5841402171573265477L;

	private Multimap<String, ConflictResolution> rules = HashMultimap.create();

	private transient IObjectNode fusedRecord = new ObjectNode();

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.fusion.ConflictResolution#fuse(eu.stratosphere.sopremo.type
	 * .IArrayNode)
	 */
	@Override
	public void fuse(IArrayNode values) {
		this.fusedRecord.clear();

		for (IJsonNode value : values) {
			IObjectNode object = (IObjectNode) value;
			for (String fieldName : object.getFieldNames()) {
				IJsonNode fieldValues = this.fusedRecord.get(fieldName);
				if (fieldValues.isMissing())
					this.fusedRecord.put(fieldName, fieldValues = new ArrayNode());
				((IArrayNode) fieldValues).add(object.get(fieldName));
			}
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
		for (Entry<String, ConflictResolution> rule : ((RecordResolution) original).rules.entries())
			this.rules.put(rule.getKey(), (ConflictResolution) rule.getValue().clone());
	}
}
