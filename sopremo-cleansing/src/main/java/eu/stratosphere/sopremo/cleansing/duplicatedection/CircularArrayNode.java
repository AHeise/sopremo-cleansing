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

import java.util.Arrays;
import java.util.Iterator;

import eu.stratosphere.sopremo.type.AbstractArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.MissingNode;

/**
 * @author Arvid Heise
 */
public class CircularArrayNode extends AbstractArrayNode {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3499312234904146409L;

	private final IJsonNode[] buffer;

	private int head, size;

	public CircularArrayNode(int size) {
		this.buffer = new IJsonNode[size];
		Arrays.fill(this.buffer, MissingNode.getInstance());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#size()
	 */
	@Override
	public int size() {
		return this.size;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#add(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IArrayNode add(IJsonNode node) {
		final int index;
		if (this.size == this.buffer.length) {
			index = this.head;
			this.head = (this.head + 1) % this.buffer.length;
		} else {
			index = (this.head + this.size) % this.buffer.length;
			this.size++;
		}
		final IJsonNode element = this.buffer[index];
		if (!element.isCopyable(node))
			this.buffer[index] = node.clone();
		else
			element.copyValueFrom(node);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#add(int, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IArrayNode add(int index, IJsonNode element) {
		throw new UnsupportedOperationException();
	}

	public IJsonNode get() {
		return this.buffer[this.head];
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#get(int)
	 */
	@Override
	public IJsonNode get(int index) {
		return this.buffer[(this.head + index) % this.buffer.length];
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#set(int, eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode set(int index, IJsonNode node) {
		IJsonNode old = this.buffer[(this.head + index) % this.buffer.length];
		this.buffer[(this.head + index) % this.buffer.length] = node;
		return old;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#remove(int)
	 */
	@Override
	public IJsonNode remove(int index) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#clear()
	 */
	@Override
	public void clear() {
		this.head = this.size = 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IStreamNode#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return this.size == 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<IJsonNode> iterator() {
		return new Iterator<IJsonNode>() {
			int index = 0;

			/*
			 * (non-Javadoc)
			 * @see java.util.Iterator#hasNext()
			 */
			@Override
			public boolean hasNext() {
				return this.index < CircularArrayNode.this.size;
			}

			/*
			 * (non-Javadoc)
			 * @see java.util.Iterator#remove()
			 */
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			/*
			 * (non-Javadoc)
			 * @see java.util.Iterator#next()
			 */
			@Override
			public IJsonNode next() {
				return CircularArrayNode.this.buffer[(CircularArrayNode.this.head + this.index++) %
					CircularArrayNode.this.buffer.length];
			}
		};
	}

}
