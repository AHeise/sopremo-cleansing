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
package eu.stratosphere.sopremo.cleansing.duplicatedection;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass;
import eu.stratosphere.sopremo.operator.Operator;

/**
 * @author arv
 *
 */
public abstract class MultipassDuplicateDetectionAlgorithm extends CompositeDuplicateDetectionAlgorithm<Blocking> {

	/**
	 * Initializes MultipassDuplicateDetectionAlgorithm.
	 *
	 */
	public MultipassDuplicateDetectionAlgorithm() {
		super();
	}

	/**
	 * Initializes MultipassDuplicateDetectionAlgorithm.
	 *
	 * @param minInputs
	 * @param maxInputs
	 * @param minOutputs
	 * @param maxOutputs
	 */
	public MultipassDuplicateDetectionAlgorithm(int minInputs, int maxInputs, int minOutputs, int maxOutputs) {
		super(minInputs, maxInputs, minOutputs, maxOutputs);
	}

	/**
	 * Initializes MultipassDuplicateDetectionAlgorithm.
	 *
	 * @param numberOfInputs
	 * @param numberOfOutputs
	 */
	public MultipassDuplicateDetectionAlgorithm(int numberOfInputs, int numberOfOutputs) {
		super(numberOfInputs, numberOfOutputs);
	}

	protected abstract Operator<?> createPass(List<Operator<?>> inputs, Pass pass, CandidateComparison comparison);

	@Override
	protected Operator<?> getImplementation(List<Operator<?>> inputs, CandidateSelection selection, CandidateComparison comparison) {
		if (selection.getPasses().size() == 1)
			return createPass(inputs, selection.getPasses().get(0), comparison);
	
		List<Operator<?>> passes = new ArrayList<Operator<?>>();
		for (Pass pass : selection.getPasses())
			passes.add(createPass(inputs, pass, comparison));
	
		return new UnionAll().withInputs(passes);
	}

}