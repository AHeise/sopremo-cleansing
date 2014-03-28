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
package eu.stratosphere.sopremo.cleansing;

import eu.stratosphere.sopremo.operator.*;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * 
 */
@InputCardinality(1)
@OutputCardinality(1)
@Name(verb = "cluster")
public class Clustering extends CompositeOperator<Clustering> {
	private Operator<?> implementation = new TransitiveClosure();

	/**
	 * Initializes Clustering.
	 */
	public Clustering() {
		this.addPropertiesFrom(this.implementation);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(eu.stratosphere.sopremo.operator.SopremoModule
	 * )
	 */
	@Override
	public void addImplementation(SopremoModule module) {
		module.embed(this.implementation);
	}

	public Algorithm getAlgorithm() {
		for (Algorithm algorithm : Algorithm.values())
			if (algorithm.getImplementation().isInstance(this.implementation))
				return algorithm;
		return null;
	}

	/**
	 * Returns the implementation.
	 * 
	 * @return the implementation
	 */
	public Operator<?> getImplementation() {
		return this.implementation;
	}

	@Property
	@Name(verb = "using", preposition = "with")
	public void setAlgorithm(Algorithm algorithm) {
		setImplementation(ReflectUtil.newInstance(algorithm.getImplementation()));
	}

	/**
	 * Sets the implementation to the specified value.
	 * 
	 * @param implementation
	 *        the implementation to set
	 */
	public void setImplementation(Operator<?> implementation) {
		if (implementation == null)
			throw new NullPointerException("implementation must not be null");

		this.removePropertiesFrom(this.implementation);
		this.implementation = implementation;
		this.addPropertiesFrom(this.implementation);
	}

	public static enum Algorithm {
		Transitive_Closure(TransitiveClosure.class);

		private Class<? extends Operator<?>> implementation;

		private Algorithm(Class<? extends Operator<?>> implementation) {
			this.implementation = implementation;
		}

		/**
		 * Returns the implementation.
		 * 
		 * @return the implementation
		 */
		public Class<? extends Operator<?>> getImplementation() {
			return this.implementation;
		}
	}
}
