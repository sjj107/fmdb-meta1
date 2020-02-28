package com.fiberhome.fmdb.meta.bean;


import org.apache.orc.TypeDescription;

import java.util.*;
import java.util.regex.Pattern;

/**
 * @Description TODO
 * @Author sjj
 * @Date 2020/2/28 21:19
 */
public class FmdbTypeDesc {
    private static final int MAX_PRECISION = 38;
    private static final int MAX_SCALE = 38;
    private static final int DEFAULT_LENGTH = 256;
    private static final int DEFAULT_PRECISION = 38;
    private static final int DEFAULT_SCALE = 10;
    static final Pattern UNQUOTED_NAMES = Pattern.compile("^[a-zA-Z0-9_]+$");

    private FmdbTypeDesc parent;
    private final Category category;
    private final List<FmdbTypeDesc> children;
    private final List<String> fieldNames;
    private final Map<String,String> attributes = new HashMap<>();
    private int maxLength = DEFAULT_LENGTH;
    private int precision = DEFAULT_PRECISION;
    private int scale = DEFAULT_SCALE;
    static void printFieldName(StringBuilder buffer, String name) {
        if (UNQUOTED_NAMES.matcher(name).matches()) {
            buffer.append(name);
        } else {
            buffer.append('`');
            buffer.append(name.replace("`", "``"));
            buffer.append('`');
        }
    }
    public FmdbTypeDesc(Category category) {
        this.category = category;
        if (category.isPrimitive) {
            children = null;
        } else {
            children = new ArrayList<>();
        }
        if (category == Category.STRUCT) {
            fieldNames = new ArrayList<>();
        } else {
            fieldNames = null;
        }
    }

    public enum Category {
        BOOLEAN("boolean", true),
        BYTE("tinyint", true),
        SHORT("smallint", true),
        INT("int", true),
        LONG("bigint", true),
        FLOAT("float", true),
        DOUBLE("double", true),
        STRING("string", true),
        DATE("date", true),
        TIMESTAMP("timestamp", true),
        BINARY("binary", true),
        DECIMAL("decimal", true),
        VARCHAR("varchar", true),
        CHAR("char", true),
        LIST("array", false),
        MAP("map", false),
        STRUCT("struct", false),
        UNION("uniontype", false),
        TIMESTAMP_INSTANT("timestamp with local time zone", false);

        Category(String name, boolean isPrimitive) {
            this.name = name;
            this.isPrimitive = isPrimitive;
        }

        final boolean isPrimitive;
        final String name;

        public boolean isPrimitive() {
            return isPrimitive;
        }

        public String getName() {
            return name;
        }
    }

    public static FmdbTypeDesc fromString(String typeName) {
        if (typeName == null) {
            return null;
        }
        ParserUtils.StringPosition source = new ParserUtils.StringPosition(typeName);
        FmdbTypeDesc result = ParserUtils.parseType(source);
        if (source.hasCharactersLeft()) {
            throw new IllegalArgumentException("Extra characters at " + source);
        }
        return result;
    }

    /**
     * For decimal types, set the precision.
     * @param precision the new precision
     * @return this
     */
    public FmdbTypeDesc withPrecision(int precision) {
        if (category != Category.DECIMAL) {
            throw new IllegalArgumentException("precision is only allowed on decimal"+
                    " and not " + category.name);
        } else if (precision < 1 || precision > MAX_PRECISION || scale > precision){
            throw new IllegalArgumentException("precision " + precision +
                    " is out of range 1 .. " + scale);
        }
        this.precision = precision;
        return this;
    }

    /**
     * For decimal types, set the scale.
     * @param scale the new scale
     * @return this
     */
    public FmdbTypeDesc withScale(int scale) {
        if (category != FmdbTypeDesc.Category.DECIMAL) {
            throw new IllegalArgumentException("scale is only allowed on decimal"+
                    " and not " + category.name);
        } else if (scale < 0 || scale > MAX_SCALE || scale > precision) {
            throw new IllegalArgumentException("scale is out of range at " + scale);
        }
        this.scale = scale;
        return this;
    }

    /**
     * Get the kind of this type.
     * @return get the category for this type.
     */
    public Category getCategory() {
        return category;
    }

    /**
     * Add a child to a union type.
     * @param child a new child type to add
     * @return the union type.
     */
    public FmdbTypeDesc addUnionChild(FmdbTypeDesc child) {
        if (category != Category.UNION) {
            throw new IllegalArgumentException("Can only add types to union type" +
                    " and not " + category);
        }
        addChild(child);
        return this;
    }

    /**
     * Add a field to a struct type as it is built.
     * @param field the field name
     * @param fieldType the type of the field
     * @return the struct type
     */
    public FmdbTypeDesc addField(String field, FmdbTypeDesc fieldType) {
        if (category != Category.STRUCT) {
            throw new IllegalArgumentException("Can only add fields to struct type" +
                    " and not " + category);
        }
        fieldNames.add(field);
        addChild(fieldType);
        return this;
    }

    /**
     * Set the maximum length for char and varchar types.
     * @param maxLength the maximum value
     * @return this
     */
    public FmdbTypeDesc withMaxLength(int maxLength) {
        if (category != Category.VARCHAR && category != Category.CHAR) {
            throw new IllegalArgumentException("maxLength is only allowed on char" +
                    " and varchar and not " + category.name);
        }
        this.maxLength = maxLength;
        return this;
    }

    /**
     * Add a child to a type.
     * @param child the child to add
     */
    public void addChild(FmdbTypeDesc child) {
        switch (category) {
            case LIST:
                if (children.size() >= 1) {
                    throw new IllegalArgumentException("Can't add more children to list");
                }
            case MAP:
                if (children.size() >= 2) {
                    throw new IllegalArgumentException("Can't add more children to map");
                }
            case UNION:
            case STRUCT:
                children.add(child);
                child.parent = this;
                break;
            default:
                throw new IllegalArgumentException("Can't add children to " + category);
        }
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        printToBuffer(buffer);
        return buffer.toString();
    }
    public void printToBuffer(StringBuilder buffer) {
        String s = category.name.toString();
//    buffer.append(category.name);
        buffer.append(s);
        String categoryS=category.toString();

        Map<String, String> hashMap = UndentifyBase.getInstance().getHashMap();

        String[] array={"BOOLEAN","BYTE","SHORT","INT","LONG","DATE","TIMESTAMP","TIMESTAMP_INSTANT","FLOAT","DOUBLE"
                ,"DECIMAL","STRING","BINARY","CHAR","VARCHAR","STRUCT","UNION","LIST","MAP"};

        List<String> list = Arrays.asList(array);

        categoryS=list.contains(categoryS)?categoryS:(hashMap.get(categoryS).toUpperCase());

        // switch (category) {
        switch (categoryS) {
            case "DECIMAL":
                buffer.append('(');
                buffer.append(precision);
                buffer.append(',');
                buffer.append(scale);
                buffer.append(')');
                break;
            case "CHAR":
            case "VARCHAR":
                buffer.append('(');
                buffer.append(maxLength);
                buffer.append(')');
                break;
            case "LIST":
            case "MAP":
            case "UNION":
                buffer.append('<');
                for(int i=0; i < children.size(); ++i) {
                    if (i != 0) {
                        buffer.append(',');
                    }
                    children.get(i).printToBuffer(buffer);
                }
                buffer.append('>');
                break;
            case "STRUCT":
                buffer.append('<');
                for(int i=0; i < children.size(); ++i) {
                    if (i != 0) {
                        buffer.append(',');
                    }
                    printFieldName(buffer, fieldNames.get(i));
                    buffer.append(':');
                    children.get(i).printToBuffer(buffer);
                }
                buffer.append('>');
                break;
            default:
                break;
        }
    }
}
