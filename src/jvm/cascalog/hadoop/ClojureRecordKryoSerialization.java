package cascalog.hadoop;

import clojure.lang.IRecord;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import clojure.lang.Reflector;
import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;

@SuppressWarnings("unchecked")
public class ClojureRecordKryoSerialization extends ClojureKryoSerialization {
    public ClojureRecordKryoSerialization() {
        super();
    }

    public ClojureRecordKryoSerialization(Configuration conf) {
        super(conf);
    }

    public Kryo decorateKryo(Kryo k) {
        Thread.currentThread().setContextClassLoader(RT.baseLoader());
        return super.decorateKryo(k);
    }

    private boolean isRecord(Class<?> aClass) {
        return IRecord.class.isAssignableFrom(aClass);
    }

    @Override
    public boolean accept(Class<?> aClass) {
        return isRecord(aClass) || super.accept(aClass);
    }

    private static final Class CONTENT_CLASS = PersistentVector.class;

    @Override
    public Serializer getSerializer(Class aClass) {
        if(!isRecord(aClass)) {
            return super.getSerializer(aClass);
        }

        final Serializer kryo = super.getSerializer(CONTENT_CLASS);
        return new Serializer() {
            @Override
            public void open(OutputStream outputStream) throws IOException {
                kryo.open(outputStream);
            }

            @Override
            public void serialize(Object o) throws IOException {
                final PersistentVector v = PersistentVector.create(new ArrayList(((Map)o).values()));
                kryo.serialize(v);
            }

            @Override
            public void close() throws IOException {
                kryo.close();
            }
        };
    }

    @Override
    public Deserializer getDeserializer(final Class aClass) {
        if(!isRecord(aClass)) {
            return super.getDeserializer(aClass);
        }

        final Deserializer kryo = super.getDeserializer(CONTENT_CLASS);
        return new Deserializer() {
            @Override
            public void open(InputStream inputStream) throws IOException {
                kryo.open(inputStream);
            }

            @Override
            public Object deserialize(Object o) throws IOException {
                final PersistentVector v = (PersistentVector)kryo.deserialize(o);
                return Reflector.invokeConstructor(aClass, v.toArray());
            }

            @Override
            public void close() throws IOException {
                kryo.close();
            }
        };
    }
}
