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

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * <code>AbstractSimilarity</code> implements {@link Similarity}. Other <code>Similarity</code> implementations should
 * derive from this class.
 * 
 * @author Matthias Pohl
 * @author Arvid Heise
 */
public abstract class AbstractSimilarity<NodeType extends IJsonNode> implements Similarity<NodeType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2458703144897619013L;

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
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#isSymmetric()
	 */
	@Override
	public boolean isSymmetric() {
		return ReflectUtil.getAnnotation(getClass(), Asymmetric.class) == null;
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
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append(this.getClass().getSimpleName());
	}
}
