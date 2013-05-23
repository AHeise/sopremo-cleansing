/*
 * DuDe - The Duplicate Detection Toolkit
 * 
 * Copyright (C) 2010  Hasso-Plattner-Institut für Softwaresystemtechnik GmbH,
 *                     Potsdam, Germany 
 *
 * This file is part of DuDe.
 * 
 * DuDe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * DuDe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with DuDe.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */

package eu.stratosphere.sopremo.cleansing.similarity;

import java.io.IOException;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * <code>AbstractSimilarity</code> implements {@link Similarity}. Other <code>Similarity</code> implementations should
 * derive from this class.
 * 
 * @author Matthias Pohl
 * @author Arvid Heise
 */
public abstract class AbstractSimilarity<NodeType extends IJsonNode> extends AbstractSopremoType implements
		Similarity<NodeType> {

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#isSymmetric()
	 */
	@Override
	public boolean isSymmetric() {
		return ReflectUtil.getAnnotation(this.getClass(), Asymmetric.class) == null;
	}

	// /*
	// * (non-Javadoc)
	// * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#getExpectedType()
	// */
	// @SuppressWarnings("unchecked")
	// @Override
	// public Class<NodeType> getExpectedType() {
	// return (Class<NodeType>) BoundTypeUtil.getBindingOfSuperclass(getClass(),
	// AbstractSimilarity.class).getParameters()[0].getType();
	// }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result;
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append(this.getClass().getSimpleName());
	}
}
