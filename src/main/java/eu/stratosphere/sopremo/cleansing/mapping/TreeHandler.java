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

import java.util.HashMap;
import java.util.Map;

public class TreeHandler<V> {
	private Map<Class<V>, NodeHandler<V>> handlers = new HashMap<Class<V>, NodeHandler<V>>();

	public NodeHandler<V> get(Class<? extends V> key) {
		return this.handlers.get(key);
	}

	@SuppressWarnings("unchecked")
	public <N extends V> void put(Class<N> key, NodeHandler<N> handler) {
		this.handlers.put((Class<V>) key, (NodeHandler<V>) handler);
	}

	@SuppressWarnings("unchecked")
	public void handle(V value) {
		NodeHandler<V> nodeHandler = get((Class<? extends V>) value.getClass());
		if(nodeHandler == null)
			throw new IllegalArgumentException("Cannot handle " + value + " (" + value.getClass() + ")");
		nodeHandler.handle(value, (TreeHandler<Object>) this);
	}		
}