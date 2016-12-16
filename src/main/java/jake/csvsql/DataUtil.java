package jake.csvsql;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jakejaehee on 2016-12-16.
 */
public class DataUtil {
    public static final Locale DEFAULT_LOCALE = new Locale("en","JP"); // Locale.US;

    /**
     * dateに日数を加える。 <b>(Korean :</b> date에 일수를 더한다<b>)</b>
     *
     * @param date
     *            {@link Date} <b>(Korean :</b> {@link Date}<b>)</b>
     * @param days
     *            日数 : 0、負の数、正の数 <b>(Korean :</b> 일수 : 0, 음수, 양수<b>)</b>
     * @return
     */
    public static Date addDays(Date date, int days) {
        if (date == null)
            return null;
        Date ret = new Date(date.getTime());
        ret.setTime(date.getTime() + ((long) days * 86400000L));
        return ret;
    }

    /**
     * dateに日数を加えた後、文字列に変換して返還する。 <b>(Korean :</b> date에 일수를 더한 후 문자열로 변환하여
     * 반환한다.<b>)</b>
     *
     * @param date
     *            {@link Date} <b>(Korean :</b> {@link Date}<b>)</b>
     * @param days
     *            日数 : 0、負の数、正の数 <b>(Korean :</b> 일수 : 0, 음수, 양수<b>)</b>
     * @param format
     *            日付文字列の形式 <b>(Korean :</b> 날짜 문자열의 형식<b>)</b>
     * @return
     */
    public static String toStringAfterAddDays(Date date, int days, String format) {
        if (date == null)
            return null;
        Date ret = addDays(date, days);
        SimpleDateFormat formatter = new SimpleDateFormat(format,
                DEFAULT_LOCALE);
        return formatter.format(ret);
    }

    /**
     * 与えられた日付が属する週の月曜日を取得する。 <b>(Korean :</b> 주어진 날짜가 속한 주의 월요일을 구한다.<b>)</b>
     *
     * @param date
     *            日付 <b>(Korean :</b> 날짜<b>)</b>
     * @return
     */
    public static Date getThisMonday(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int day = cal.get(Calendar.DAY_OF_WEEK);
        if (day == Calendar.TUESDAY) {
            return addDays(date, -1);
        } else if (day == Calendar.WEDNESDAY) {
            return addDays(date, -2);
        } else if (day == Calendar.THURSDAY) {
            return addDays(date, -3);
        } else if (day == Calendar.FRIDAY) {
            return addDays(date, -4);
        } else if (day == Calendar.SATURDAY) {
            return addDays(date, -5);
        } else if (day == Calendar.SUNDAY) {
            return addDays(date, -6);
        }
        return date;
    }

    /**
     * 与えられた日付が属する週の日曜日を取得する。 <b>(Korean :</b> 주어진 날짜가 속한 주의 일요일을 구한다.<b>)</b>
     *
     * @param date
     *            日付 <b>(Korean :</b> 날짜<b>)</b>
     * @return
     */
    public static Date getThisSunday(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int day = cal.get(Calendar.DAY_OF_WEEK);
        if (day == Calendar.MONDAY) {
            return addDays(date, 6);
        } else if (day == Calendar.TUESDAY) {
            return addDays(date, 5);
        } else if (day == Calendar.WEDNESDAY) {
            return addDays(date, 4);
        } else if (day == Calendar.THURSDAY) {
            return addDays(date, 3);
        } else if (day == Calendar.FRIDAY) {
            return addDays(date, 2);
        } else if (day == Calendar.SATURDAY) {
            return addDays(date, 1);
        }
        return date;
    }

    /**
     * 주어진 날짜가 속한 달의 초일을 구한다.
     *
     * @param date
     *            日付 <b>(Korean :</b> 날짜<b>)</b>
     * @return date
     */
    public static Date getFirstDateMonth(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        cal.set(Calendar.DATE, cal.getActualMinimum(Calendar.DAY_OF_MONTH));


        return cal.getTime();
    }

    /**
     * 주어진 날짜가 속한 달의 말일를 구한다.
     *
     * @param date
     *            日付 <b>(Korean :</b> 날짜<b>)</b>
     * @return date
     */
    public static Date getLastDateMonth(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        cal.set(Calendar.DATE, cal.getActualMaximum(Calendar.DAY_OF_MONTH));

        return cal.getTime();
    }

    /**
     * Listから数字カラムの値を与えられた小数点の桁(decimalPlaceToPrint)に変換する。<b>(Korean :</b>
     * List에서 숫자 칼럼의 값을 주어진 소수점 자리수(decimalPlaceToPrint)로 변환한다.<b>)</b>
     *
     * @param list
     *            List
     * @param numberColId
     *            数字カラムキー <b>(Korean :</b> 숫자 칼럼 키<b>)</b>
     * @param decimalPlaceToPrint
     *            小数点の桁 <b>(Korean :</b> 소수점 자리수<b>)</b>
     */
    public static void decimalPlace(List<Map> list, final String numberColId,
                                    final int decimalPlaceToPrint) {
        ListSql.handleRecords(list, new RecordHandler() {
            public void handle(int index, Map row) {
                Object num = row.get(numberColId);
                if (num != null) {
                    String str = toNumberString(num, decimalPlaceToPrint);
                    row.put(numberColId, str);
                }
            }
        });
    }

    /**
     * numberを小数点の桁decimalPlaceToRoundから四捨五入して与えられた小数点の桁だけ作成し返す。 <b>(Korean
     * :</b> number를 소수점 자리수 decimalPlaceToRound에서 반올림하고 주어진 소수점 자리수 만큼 만들어
     * 반환한다.<b>)</b>
     *
     * @param number
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @param decimalPlaceToRound
     *            四捨五入する小数点の桁 <b>(Korean :</b> 반올림할 소수점 자리수<b>)</b>
     * @param decimalPlaceToPrint
     *            作成する小数点の桁 <b>(Korean :</b> 만들 소수점 자리수<b>)</b>
     * @return
     */
    public static String round(Object number, int decimalPlaceToRound,
                               int decimalPlaceToPrint) {
        double d = toDouble(number);
        double r = round(d, decimalPlaceToRound);
        return toNumberString(r, decimalPlaceToPrint);
    }

    /**
     * numberを小数点の桁decimalPlaceToRoundから四捨五入する。 <b>(Korean :</b> number를 소수점 자리수
     * decimalPlaceToRound에서 반올림한다.<b>)</b>
     *
     * @param number
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @param decimalPlace
     *            四捨五入する小数点の桁 <b>(Korean :</b> 반올림할 소수점 자리수<b>)</b>
     * @return
     */
    public static double round(Object number, int decimalPlace) {
        double d = toDouble(number);
        double a = 1;
        for (int i = 1; i < decimalPlace; i++)
            a *= 10;
        return Math.round(d * a) / a;
    }

    /**
     * numberの小数点の桁を指定する。 <b>(Korean :</b> number를 소수점 자리수를 정한다.<b>)</b>
     *
     * @param number
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @param decimalPlace
     *            小数点の桁 <b>(Korean :</b> 소수점 자리수<b>)</b>
     * @return
     */
    final public static String toNumberString(Object number, int decimalPlace) {
        double d = toDouble(number);
        return String.format("%." + decimalPlace + "f", d);
    }

    /**
     * textがRegular Expressionに満たしていることを確認する。 <b>(Korean :</b> text가 Regular
     * Expression에 부합하는지 검사한다.<b>)</b>
     *
     * @param text
     *            Text
     * @param regExpr
     *            Regular Expression
     * @return
     */
    public static boolean regExprMatch(String text, String regExpr) {
        Pattern p = Pattern.compile(regExpr);
        Matcher m = p.matcher(text);
        return m.matches();
    }

    /**
     * Performs a wildcard matching for the text and pattern provided.<BR>
     * <BR>
     * <B>Examples</B>.<BR>
     * &nbsp;&nbsp;&nbsp;test="jakelee", If pattern="*jakelee*" then true,<BR>
     * &nbsp;&nbsp;&nbsp;"*jakelee" is true,<BR>
     * &nbsp;&nbsp;&nbsp;"jakelee*" is true,<BR>
     * &nbsp;&nbsp;&nbsp;"*akelee" is true<BR>
     * &nbsp;&nbsp;&nbsp;"jakele*" is true<BR>
     * &nbsp;&nbsp;&nbsp;"j*akelee" is true<BR>
     * &nbsp;&nbsp;&nbsp;"*ak elee" is false<BR>
     * &nbsp;&nbsp;&nbsp;" *akelee" is false<BR>
     *
     * @param str1
     *            the text to be tested for matches.
     *
     * @param str2
     *            the pattern to be matched for. This can contain the wildcard
     *            character '*' (asterisk).
     *
     * @return <tt>true</tt> if a match is found, <tt>false</tt> otherwise.
     */
    public static boolean wildCardMatch(String str1, String str2) {
        String text = str1;
        String pattern = str2;
        if (str1 != null
                && (str1.indexOf("*") != -1 || str1.indexOf("?") != -1)) {
            text = str2;
            pattern = str1;
        }

        if ((text == pattern) || (isEmpty(text) && isEmpty(pattern)))
            return true;

        if (isEmpty(text) || isEmpty(pattern))
            return false;

        if (pattern.indexOf("*") == -1 && pattern.indexOf("?") == -1) {
            return pattern.equals(text);
        }

        pattern = pattern.replaceAll(String.valueOf((char) 65290), "*");
        String[] cards = pattern.split("\\*");

        char firstChar = pattern.charAt(0);
        char lastChar = pattern.charAt(pattern.length() - 1);
        if (firstChar != '*' && lastChar != '*' && cards.length == 1) {
            return equalsWithQuestionMarkPattern(text, cards[0]);
        } else {
            String src = text;
            int offset = 0;
            for (int i = 0; i < cards.length; i++) {
                String card = cards[i];
                if (!"".equals(card)) {
                    if (i == 0) {
                        if (!startsWithQuestionMarkPattern(src, offset, card))
                            return false;
                        offset += card.length();
                    } else if (i == cards.length - 1 && lastChar != '*') {
                        offset = src.length() - card.length();
                        return equalsWithQuestionMarkPattern(src, offset, card);
                    } else {
                        int idx = indexWithQuestionMarkPattern(src, offset,
                                card);
                        if (idx == -1)
                            return false;
                        offset = (idx + card.length());
                    }
                }
            }
        }
        return true;
    }

    /**
     *
     * @param text
     * @param pattern
     * @return
     */
    private static int indexWithQuestionMarkPattern(String text, String pattern) {
        return indexWithQuestionMarkPattern(text, 0, pattern);
    }

    private static int indexWithQuestionMarkPattern(String text, int offset,
                                                    String pattern) {
        int tLen = text == null ? 0 : text.length();
        if (tLen - offset < (pattern == null ? 0 : pattern.length())) {
            return -1;
        }
        for (int t = offset; t < tLen; t++) {
            if (questionMarkMatch(text, t, pattern, false))
                return t;
        }
        return -1;
    }

    private static boolean equalsWithQuestionMarkPattern(String text,
                                                         String pattern) {
        return questionMarkMatch(text, 0, pattern, true);
    }

    private static boolean equalsWithQuestionMarkPattern(String text,
                                                         int offset, String pattern) {
        return questionMarkMatch(text, offset, pattern, true);
    }

    private static boolean startsWithQuestionMarkPattern(String text,
                                                         String pattern) {
        return questionMarkMatch(text, 0, pattern, false);
    }

    private static boolean startsWithQuestionMarkPattern(String text,
                                                         int offset, String pattern) {
        return questionMarkMatch(text, offset, pattern, false);
    }

    private static boolean questionMarkMatch(String text, int offset,
                                             String pattern, boolean mustSameLen) {
        if (offset == 0
                && ((text == pattern) || ("".equals(text) && pattern == null) || (text == null && ""
                .equals(pattern))))
            return true;
        if (offset < 0)
            return false;

        int tLen = text == null ? 0 : text.length();
        int pLen = pattern == null ? 0 : pattern.length();
        if (mustSameLen) {
            if (tLen - offset != pLen)
                return false;
        } else {
            if (tLen - offset < pLen)
                return false;
        }

        for (int t = offset, p = 0; t < tLen && p < pLen; t++, p++) {
            char pCh = pattern.charAt(p);
            if (text.charAt(t) != pCh && pCh != '?')
                return false;
        }
        return true;
    }

    /**
     * 文字列がnullまたは""であることを検査する。 <b>(Korean :</b> 문자열이 null 또는 "" 인지 검사<b>)</b>
     *
     * @param string
     *            文字列 <b>(Korean :</b> 문자열<b>)</b>
     * @return
     */
    public static final boolean isEmpty(Object string) {
        return string == null || "".equals(string);
    }

    /**
     * numberをint型に変換する。 <b>(Korean :</b> number를 int 형으로 변환한다.<b>)</b>
     *
     * @param number
     * @return
     */
    public static int toInt(Object number) {
        return toInt(number, 0);
    }

    /**
     * numberをlong型に変換する。 <b>(Korean :</b> number를 long 형으로 변환한다.<b>)</b>
     *
     * @param number
     * @return
     */
    public static long toLong(Object number) {
        return toLong(number, 0l);
    }

    /**
     * numberをlong型に変換する。変換できない場合はデフォルト値(defaultValue)を返す。 <b>(Korean :</b>
     * number를 long 형으로 변환한다. 변환할 수 없을 경우 기본값(defaultValue)를 반환한다.<b>)</b>
     *
     * @param number
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @param defaultVal
     *            変換できない場合返すデフォルト値 <b>(Korean :</b> 변환할 수 없는 경우에 반환할 기본값<b>)</b>
     * @return
     */
    public static long toLong(Object number, long defaultVal) {
        long num = defaultVal;
        if (number instanceof Integer) {
            num = (Integer) number;
        } else if (number instanceof Double) {
            num = (Integer) number;
        } else if (number instanceof Float) {
            num = (Integer) number;
        } else if (number instanceof String && !"".equals((String) number)) {
            String str = ((String) number).replace(",", "");
            try {
                double d = Double.parseDouble(str);
                num = (long) d;
            } catch (Throwable t) {
                float f = Float.parseFloat(str);
                num = (long) f;
            }
        }
        return num;
    }

    /**
     * numberをshort型に変換する。変換できない場合はデフォルト値(defaultValue)を返す。 <b>(Korean :</b>
     * number를 short 형으로 변환한다. 변환할 수 없을 경우 기본값(defaultValue)를 반환한다.<b>)</b>
     *
     * @param number
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @param defaultVal
     *            変換できない場合返すデフォルト値 <b>(Korean :</b> 변환할 수 없는 경우에 반환할 기본값<b>)</b>
     * @return
     */
    public static short toShort(Object number, short defaultVal) {
        short num = defaultVal;
        if (number instanceof Short) {
            num = (Short) number;
        } else if (number instanceof Integer) {
            num = (Short) number;
        } else if (number instanceof Double) {
            num = (Short) number;
        } else if (number instanceof String && !"".equals((String) number)) {
            double d = Double.parseDouble(((String) number).replace(",", ""));
            num = (short) d;
        }
        return num;
    }

    /**
     * numberをint型に変換する。変換できない場合はデフォルト値(defaultValue)を返す。 <b>(Korean :</b>
     * number를 int 형으로 변환한다. 변환할 수 없을 경우 기본값(defaultValue)를 반환한다.<b>)</b>
     *
     * @param number
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @param defaultVal
     *            変換できない場合返すデフォルト値 <b>(Korean :</b> 변환할 수 없는 경우에 반환할 기본값<b>)</b>
     * @return
     */
    public static int toInt(Object number, int defaultVal) {
        int num = defaultVal;
        if (number instanceof Integer) {
            num = (Integer) number;
        } else if (number instanceof Double) {
            num = (Integer) number;
        } else if (number instanceof String && !"".equals((String) number)) {
            double d = Double.parseDouble(((String) number).replace(",", ""));
            num = (int) d;
        }
        return num;
    }

    /**
     * numberをint型に変換する。変換できない場合はデフォルト値(defaultValue)を返す。 <b>(Korean :</b>
     * number를 double 형으로 변환한다. 변환할 수 없을 경우 기본값(defaultValue)를 반환한다.<b>)</b>
     *
     * @param number
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @param defaultVal
     *            変換できない場合返すデフォルト値 <b>(Korean :</b> 변환할 수 없는 경우에 반환할 기본값<b>)</b>
     * @return
     */
    public static double toDouble(Object number, double defaultVal) {
        double num = defaultVal;
        if (number instanceof Integer) {
            num = (Integer) number;
        } else if (number instanceof Double) {
            num = (Double) number;
        } else if (number instanceof Float) {
            num = (Float) number;
        } else if (number instanceof String && !"".equals((String) number)) {
            num = Double.parseDouble(((String) number).replace(",", ""));
        }
        return num;
    }

    /**
     * numberをdouble型に変換する。 <b>(Korean :</b> number를 double 형으로 변환한다.<b>)</b>
     *
     * @param number
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @return
     */
    public static double toDouble(Object number) {
        return toDouble(number, 0.0);
    }

    /**
     * boolをboolean型に変換する。変換できない場合はデフォルト値(defaultValue)を返すXXXXX <b>(Korean :</b>
     * bool를 boolean 형으로 변환한다. 변환할 수 없을 경우 기본값(defaultValue)를 반환한다.<b>)</b>
     *
     * @param bool
     *            true, false, "true", "false"
     * @param defaultVal
     *            変換できない場合返すデフォルト値 <b>(Korean :</b> 변환할 수 없는 경우에 반환할 기본값<b>)</b>
     * @return
     */
    public static boolean toBoolean(Object bool, boolean defaultVal) {
        boolean ret = defaultVal;
        if (bool instanceof Boolean)
            ret = (Boolean) bool;
        else if (bool instanceof String)
            ret = Boolean.parseBoolean((String) bool);
        return ret;
    }

    /**
     * Return a * b
     *
     * @param a
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @param b
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @return
     */
    public static double multiply(double a, double b) {
        int decimalPlace = getDecimalPlaceSize(a) + getDecimalPlaceSize(b);
        return Double.parseDouble(round(a * b, decimalPlace + 1,
                decimalPlace));
    }

    /**
     * 小数点の桁を取得する。 <b>(Korean :</b> 소수점 자리수를 구한다.<b>)</b>
     *
     * @param d
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @return
     */
    public static int getDecimalPlaceSize(double d) {
        String number = String.valueOf(d);
        return getDecimalPlaceSize(number);
    }

    /**
     * 小数点の桁を取得する。 <b>(Korean :</b> 소수점 자리수를 구한다.<b>)</b>
     *
     * @param d
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @return
     */
    public static int getDecimalPlaceSize(int d) {
        String number = String.valueOf(d);
        return getDecimalPlaceSize(number);
    }

    /**
     * 小数点の桁を取得する。 <b>(Korean :</b> 소수점 자리수를 구한다.<b>)</b>
     *
     * @param d
     *            数字 <b>(Korean :</b> 숫자<b>)</b>
     * @return
     */
    public static int getDecimalPlaceSize(long d) {
        String number = String.valueOf(d);
        return getDecimalPlaceSize(number);
    }

    /**
     * 小数点の桁を取得する。 <b>(Korean :</b> 소수점 자리수를 구한다.<b>)</b>
     *
     * @param number
     *            数字形式の文字列 <b>(Korean :</b> 숫자형식 문자열<b>)</b>
     * @return
     */
    public static int getDecimalPlaceSize(String number) {
        int idx = number.indexOf('.');
        if (idx >= 0) {
            if (number.endsWith(".0"))
                return 0;
            return number.length() - idx - 1;
        }
        return 0;
    }

}
