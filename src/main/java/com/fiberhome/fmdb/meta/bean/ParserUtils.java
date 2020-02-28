package com.fiberhome.fmdb.meta.bean;

/**
 * @Description TODO
 * @Author sjj
 * @Date 2020/2/28 21:22
 */
public class ParserUtils {
    static FmdbTypeDesc.Category parseCategory(ParserUtils.StringPosition source) {
        StringBuilder word = new StringBuilder();
        boolean hadSpace = true;
        while (source.position < source.length) {
            char ch = source.value.charAt(source.position);
            if (Character.isLetter(ch)) {
                word.append(Character.toLowerCase(ch));
                hadSpace = false;
            } else if (ch == ' ') {
                if (!hadSpace) {
                    hadSpace = true;
                    word.append(ch);
                }
            } else {
                break;
            }
            source.position += 1;
        }
        String catString = word.toString();
        // if there were trailing spaces, remove them.
        if (hadSpace) {
            catString = catString.trim();
        }
        if (!catString.isEmpty() /*&& !"struct".equals(catString)*/ ) {

            for (FmdbTypeDesc.Category cat : FmdbTypeDesc.Category.values()) {
                if (cat.getName().equals(catString)) {
                    return cat;
                }
            }
            // 动态添加 Enum TODO
   /*   if (!"struct".equals(catString)){
        // 转大写
        DynamicEnumUtil.addEnum(TypeDescription.Category.class, catString.toUpperCase(), new Class<?>[]{String.class, boolean.class}, new Object[]{catString, true});
      }

      for (TypeDescription.Category cat : TypeDescription.Category.values()) {
        if (cat.getName().equals(catString)) {
          return cat;
        }
      }*/
        }
        throw new IllegalArgumentException("Can't parse category at " + source + catString);
    }

    public static class StringPosition {
        final String value;
        int position;
        final int length;

        public StringPosition(String value) {
            this.value = value == null ? "" : value;
            position = 0;
            length = this.value.length();
        }

        @Override
        public String toString() {
            return '\'' + value.substring(0, position) + '^' +
                    value.substring(position) + '\'';
        }

        public String fromPosition(int start) {
            return value.substring(start, this.position);
        }

        public boolean hasCharactersLeft() {
            return position != length;
        }
    }

    public static FmdbTypeDesc parseType(ParserUtils.StringPosition source) {
        FmdbTypeDesc result = new FmdbTypeDesc(parseCategory(source));
//    System.out.println("result "+result+"--------------");
        boolean flag=false;
        String category = result.getCategory().toString();
        switch (category) {
            case "BINARY":
            case "BOOLEAN":
            case "BYTE":
            case "DATE":
            case "DOUBLE":
            case "FLOAT":
            case "INT":
            case "LONG":
            case "SHORT":
            case "STRING":
            case "TIMESTAMP":
            case "TIMESTAMP_INSTANT":
                break;
            case "CHAR":
            case "VARCHAR":
                requireChar(source, '(');
                result.withMaxLength(parseInt(source));
                requireChar(source, ')');
                break;
            case "DECIMAL": {
                requireChar(source, '(');
                int precision = parseInt(source);
                requireChar(source, ',');
                result.withScale(parseInt(source));
                result.withPrecision(precision);
                requireChar(source, ')');
                break;
            }
            case "LIST": {
                requireChar(source, '<');
                FmdbTypeDesc child = parseType(source);
                result.addChild(child);
                requireChar(source, '>');
                break;
            }
            case "MAP": {
                requireChar(source, '<');
                FmdbTypeDesc keyType = parseType(source);
                result.addChild(keyType);
                requireChar(source, ',');
                FmdbTypeDesc valueType = parseType(source);
                result.addChild(valueType);
                requireChar(source, '>');
                break;
            }
            case "UNION":
                parseUnion(result, source);
                break;
            case "STRUCT":
                parseStruct(result, source);
                break;
            default:
                for (FmdbTypeDesc.Category cat : FmdbTypeDesc.Category.values()) {
                    if (cat.getName().equalsIgnoreCase(category)) {
                        flag=true;
                        //    break;
                    }
                }
                if (flag){
                    break;
                }else {
                    throw new IllegalArgumentException("Unknown type " +
                            result.getCategory() + " at " + source);
                }

        }
        return result;
    }


    private static void parseStruct(FmdbTypeDesc type,
                                    ParserUtils.StringPosition source) {
        requireChar(source, '<');
        boolean needComma = false;
        while (!consumeChar(source, '>')) {
            if (needComma) {
                requireChar(source, ',');
            } else {
                needComma = true;
            }
            String fieldName = parseName(source);
            requireChar(source, ':');
            type.addField(fieldName, parseType(source));
        }
    }

    static String parseName(ParserUtils.StringPosition source) {
        if (source.position == source.length) {
            throw new IllegalArgumentException("Missing name at " + source);
        }
        final int start = source.position;
        if (source.value.charAt(source.position) == '`') {
            source.position += 1;
            StringBuilder buffer = new StringBuilder();
            boolean closed = false;
            while (source.position < source.length) {
                char ch = source.value.charAt(source.position);
                source.position += 1;
                if (ch == '`') {
                    if (source.position < source.length &&
                            source.value.charAt(source.position) == '`') {
                        source.position += 1;
                        buffer.append('`');
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    buffer.append(ch);
                }
            }
            if (!closed) {
                source.position = start;
                throw new IllegalArgumentException("Unmatched quote at " + source);
            } else if (buffer.length() == 0) {
                throw new IllegalArgumentException("Empty quoted field name at " + source);
            }
            return buffer.toString();
        } else {
            while (source.position < source.length) {
                char ch = source.value.charAt(source.position);
                if (!Character.isLetterOrDigit(ch) && ch != '_') {
                    break;
                }
                source.position += 1;
            }
            if (source.position == start) {
                throw new IllegalArgumentException("Missing name at " + source);
            }
            return source.value.substring(start, source.position);
        }
    }
    private static void parseUnion(FmdbTypeDesc type,
                                   ParserUtils.StringPosition source) {
        requireChar(source, '<');
        do {
            type.addUnionChild(parseType(source));
        } while (consumeChar(source, ','));
        requireChar(source, '>');
    }


    private static boolean consumeChar(ParserUtils.StringPosition source,
                                       char ch) {
        boolean result = source.position < source.length &&
                source.value.charAt(source.position) == ch;
        if (result) {
            source.position += 1;
        }
        return result;
    }
    static int parseInt(ParserUtils.StringPosition source) {
        int start = source.position;
        int result = 0;
        while (source.position < source.length) {
            char ch = source.value.charAt(source.position);
            if (!Character.isDigit(ch)) {
                break;
            }
            result = result * 10 + (ch - '0');
            source.position += 1;
        }
        if (source.position == start) {
            throw new IllegalArgumentException("Missing integer at " + source);
        }
        return result;
    }

    static void requireChar(ParserUtils.StringPosition source, char required) {
        if (source.position >= source.length ||
                source.value.charAt(source.position) != required) {
            throw new IllegalArgumentException("Missing required char '" +
                    required + "' at " + source);
        }
        source.position += 1;
    }
}
