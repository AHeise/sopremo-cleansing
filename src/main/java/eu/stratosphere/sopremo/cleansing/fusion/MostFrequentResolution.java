package eu.stratosphere.sopremo.cleansing.fusion;

import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.MissingNode;

@Name(noun = "mostFrequent")
@DefaultSerializer(value = MostFrequentResolution.MostFrequentResolutionSerializer.class)
public class MostFrequentResolution extends ConflictResolution {
	public static class MostFrequentResolutionSerializer extends
			Serializer<MostFrequentResolution> {
		FieldSerializer<MostFrequentResolution> fieldSerializer;

		public MostFrequentResolutionSerializer(final Kryo kryo,
				final Class<MostFrequentResolution> type) {
			this.fieldSerializer = new FieldSerializer<MostFrequentResolution>(kryo,
				type);
		}

		@Override
		public void write(final Kryo kryo,
				final com.esotericsoftware.kryo.io.Output output,
				final MostFrequentResolution object) {
			this.fieldSerializer.write(kryo, output, object);
			final Map<IJsonNode, Double> backingMapCopy = new HashMap<IJsonNode, Double>();
			backingMapCopy.putAll(object.histogram);
			kryo.writeObject(output, backingMapCopy, new MapSerializer());
		}

		@Override
		public MostFrequentResolution read(final Kryo kryo, final Input input,
				final Class<MostFrequentResolution> type) {
			final MostFrequentResolution object = this.fieldSerializer.read(kryo, input,
				type);
			final MapSerializer mapSerializer = new MapSerializer();
			mapSerializer.setKeyClass(IJsonNode.class, null);
			mapSerializer.setValueClass(Double.class, null);
			@SuppressWarnings("unchecked")
			final Map<IJsonNode, Double> backingMapCopy = kryo.readObject(input,
				HashMap.class, mapSerializer);
			object.histogram.putAll(backingMapCopy);

			return object;
		}

		@Override
		public MostFrequentResolution copy(final Kryo kryo,
				final MostFrequentResolution original) {
			final MostFrequentResolution copy = this.fieldSerializer.copy(kryo,
				original);
			copy.histogram.putAll(original.histogram);
			return copy;
		}
	}

	private transient final Object2DoubleMap<IJsonNode> histogram = new Object2DoubleOpenHashMap<IJsonNode>();

	@Override
	public void fuse(final IArrayNode<IJsonNode> values,
			final Map<String, CompositeEvidence> weights) {
		this.histogram.clear();
		for (int index = 0; index < values.size(); index++) {
			final IJsonNode value = this.getValueFromSourceTaggedObject(values.get(index));
			this.histogram.put(
				value,
				this.histogram.get(value) != null ? this.histogram
					.get(value)
					+ this.getWeightForValue(values.get(index), weights)
					: this.getWeightForValue(values.get(index), weights));
		}

		final ObjectSet<Object2DoubleMap.Entry<IJsonNode>> entrySet = this.histogram
			.object2DoubleEntrySet();
		double max = 0;
		IJsonNode maxObject = MissingNode.getInstance();
		for (final Object2DoubleMap.Entry<IJsonNode> entry : entrySet)
			if (entry.getDoubleValue() > max) {
				max = entry.getDoubleValue();
				maxObject = entry.getKey();
			}
		values.clear();
		if (maxObject != MissingNode.getInstance())
			values.add(maxObject);
	}
}
