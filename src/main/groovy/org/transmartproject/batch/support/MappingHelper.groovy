package org.transmartproject.batch.support

import com.google.common.base.Function
import org.codehaus.groovy.runtime.DefaultGroovyMethods
import org.codehaus.groovy.runtime.StringGroovyMethods

/**
 *
 */
class MappingHelper {

    static final Set<Class>  NUMERIC_TYPES = [
        Long.TYPE,
        Integer.TYPE,
        Short.TYPE,
        Byte.TYPE,
        Double.TYPE,
        Float.TYPE,
    ]

    private static boolean isNumeric(MetaProperty prop) {
        return Number.class.isAssignableFrom(prop.getType()) || NUMERIC_TYPES.contains(prop.getType())
    }

    static <T> T asType(Map map, Class<T> clazz) {
        MetaClass cl = DefaultGroovyMethods.getMetaClass(clazz)
        map.keySet().each {
            MetaProperty prop = cl.getMetaProperty(it)

            Object value = map.get(it)
            if (isNumeric(prop) && String.isInstance(value)) {
                //fix value, as the default cast behaviour is not what we want
                map.put(it, StringGroovyMethods.asType(value, prop.type))
            }
        }

        DefaultGroovyMethods.asType(map, clazz)
    }

    private static Map asPropertyMap(String line, List<String> props) {
        List<String> parts = parseValues(line)
        int valueCount = Math.min(parts.size(), props.size()) //we want to discard, yet allow extra columns
        Map result = [:]
        for (int i=0; i<valueCount; i++) {
            result.put(props[i], parts[i]);
        }
        result
    }

    static <T> T parseObject(String line, Class<T> clazz, List<String> props) {
        asType(asPropertyMap(line, props), clazz)
    }

    static <T> List<T> parseObjects(InputStream input, Function<String, T> mapper, LineListener listener) {

        List<T> result = []
        input.eachLine { line, idx ->
            if (idx > 1) {
                result.add(mapper.apply(line))
                if (listener) {
                    listener.lineRead(line)
                }
            }
        }
        return result
    }

    static List<String> parseValues(String line) {
        line.split('\t').toList()
    }

    static File getOptionalFile(String dataLocation, String filename) {
        if (!filename) {
            return null
        }
        if ('x'.equalsIgnoreCase(filename)) {
            //alias for 'not defined'
            return null
        }
        if (!dataLocation) {
            throw new IllegalArgumentException('Data location not defined')
        }
        new File(dataLocation, filename)
    }

}
