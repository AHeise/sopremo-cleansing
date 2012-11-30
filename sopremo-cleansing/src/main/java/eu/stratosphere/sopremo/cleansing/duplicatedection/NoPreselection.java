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

import java.io.IOException;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author arv
 *
 */
public final class NoPreselection extends Preselection {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8632780096174691250L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#clone()
	 */
	@Override
	public NoPreselection clone() {
		return (NoPreselection) super.clone();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return this;
	}
	
	@Override
	public boolean shouldProcess(IJsonNode left, IJsonNode right) {
		return true;
	}
}