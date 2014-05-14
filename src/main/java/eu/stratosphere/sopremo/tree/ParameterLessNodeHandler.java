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
package eu.stratosphere.sopremo.tree;

/**
 * 
 */
public abstract class ParameterLessNodeHandler<V, R> extends NodeHandler<V, R, Object> {
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.tree.NodeHandler#handle(java.lang.Object, java.lang.Object,
	 * eu.stratosphere.sopremo.tree.TreeHandler)
	 */
	@Override
	public R handle(V value, Object param, TreeHandler<Object, R, Object> treeHandler) {
		return handle(value, treeHandler);
	}

	protected abstract R handle(V value, TreeHandler<Object, R, Object> treeHandler);
}
