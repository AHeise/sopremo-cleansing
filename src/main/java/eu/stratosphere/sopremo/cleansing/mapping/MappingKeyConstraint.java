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

import it.unibas.spicy.model.datasource.KeyConstraint;
import it.unibas.spicy.model.paths.PathExpression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;

public class MappingKeyConstraint extends AbstractSopremoType {
	private String nesting;

	private String attribute;

	MappingKeyConstraint() {

	}

	public MappingKeyConstraint(final String nesting, final String attribute) {
		super();
		this.nesting = nesting;
		this.attribute = attribute;
	}

	public String getNesting() {
		return this.nesting;
	}

	public String getAttribute() {
		return this.attribute;
	}

	public KeyConstraint generateSpicyType() {
		final List<String> list = new ArrayList<String>();
		list.add(this.nesting);
		list.add(this.attribute);

		final PathExpression key = new PathExpression(list);

		final List<PathExpression> keyPath = new ArrayList<PathExpression>();
		keyPath.add(key);

		return new KeyConstraint(keyPath, true);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.attribute.hashCode();
		result = prime * result + this.nesting.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final MappingKeyConstraint other = (MappingKeyConstraint) obj;
		return this.nesting.equals(other.nesting) && this.attribute.equals(other.attribute);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		appendable.append("MappingKeyConstraint [nesting=");
		appendable.append(this.nesting);
		appendable.append(", attribute=");
		appendable.append(this.attribute);
		appendable.append("]");
	}

}
