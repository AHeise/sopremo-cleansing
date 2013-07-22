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
package eu.stratosphere.sopremo.cleansing.similarity;

import java.util.Collections;
import java.util.List;

import eu.stratosphere.util.dag.ConnectionNavigator;

/**
 * @author Arvid Heise
 */
public class SimilarityNavigator implements ConnectionNavigator<Similarity<?>> {
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.dag.ConnectionNavigator#getConnectedNodes(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public List<? extends Similarity<?>> getConnectedNodes(Similarity<?> node) {
		if (node instanceof CompoundSimilarity)
			return ((CompoundSimilarity<?, ? extends Similarity<?>>) node).getSubsimilarities();
		return Collections.emptyList();
	}
}
