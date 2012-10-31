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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author Arvid Heise
 *
 */
public abstract class RecordResolution extends ConflictResolution {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5841402171573265477L;
	
	private Multimap<String, ConflictResolution> rules = HashMultimap.create();
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.fusion.ConflictResolution#fuse(eu.stratosphere.sopremo.type.IArrayNode, double[], eu.stratosphere.sopremo.cleansing.fusion.FusionContext)
	 */
	@Override
	public void fuse(IArrayNode values, IJsonNode target, double[] weights, FusionContext context) {
		IObjectNode fusedRecord = SopremoUtil.reinitializeTarget(target, ObjectNode.class);
		for (IJsonNode value : values) {
			IObjectNode object = (IObjectNode) value;
			for(String fieldName : object.getFieldNames()) {
				 IJsonNode fieldValues = fusedRecord.get(fieldName);
				if(fieldValues.isMissing()) 
					fusedRecord.put(fieldName, fieldValues = new ArrayNode());
				((IArrayNode) fieldValues).add(object.get(fieldName));
			}
		}
	}
}
