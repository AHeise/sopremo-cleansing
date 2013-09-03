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

		public MostFrequentResolutionSerializer(Kryo kryo,
				Class<MostFrequentResolution> type) {
			fieldSerializer = new FieldSerializer<MostFrequentResolution>(kryo,
					type);
		}

		@Override
		public void write(Kryo kryo,
				com.esotericsoftware.kryo.io.Output output,
				MostFrequentResolution object) {
			fieldSerializer.write(kryo, output, object);
			Map<IJsonNode, Double> backingMapCopy = new HashMap<IJsonNode, Double>();
			backingMapCopy.putAll(object.histogram);
			kryo.writeObject(output, backingMapCopy, new MapSerializer());
		}

		@Override
		public MostFrequentResolution read(Kryo kryo, Input input,
				Class<MostFrequentResolution> type) {
			MostFrequentResolution object = fieldSerializer.read(kryo, input,
					type);
			MapSerializer mapSerializer = new MapSerializer();
			mapSerializer.setKeyClass(IJsonNode.class, null);
			mapSerializer.setValueClass(Double.class, null);
			@SuppressWarnings("unchecked")
			Map<IJsonNode, Double> backingMapCopy = kryo.readObject(input,
					HashMap.class, mapSerializer);
			object.histogram.putAll(backingMapCopy);

			return object;
		}

		@Override
		public MostFrequentResolution copy(Kryo kryo,
				MostFrequentResolution original) {
			MostFrequentResolution copy = this.fieldSerializer.copy(kryo,
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
			IJsonNode value = getValueFromSourceTaggedObject(values.get(index));
			this.histogram.put(
					value,
					(this.histogram.get(value) != null) ? this.histogram
							.get(value)
							+ getWeightForValue(values.get(index), weights)
							: getWeightForValue(values.get(index), weights));
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
