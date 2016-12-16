package jake.csvsql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import elastic.util.util.CommonUtil;
import elastic.util.util.StringUtil;

/**
 * ListSqlで使用される各種helper methodの集まりである。(<b>Korean</b> : ListSql에서 사용되는 각종 helper
 * method들의 모음이다.)
 * 
 * @author Jake Lee
 */
public class ListSqlUtil {

	/**
	 * valueがnullもしくは゛゛の場合にはtrueを返し、その以外の場合にはfalseを返す。(<b>Korean</b> : value가
	 * null 또는 ""이면 true를 반환하고 아니면 false를 반환한다.)
	 * 
	 * @param value
	 * @return
	 */
	public static boolean isEmpty(Object value) {
		return value == null || "".equals(value);
	}

	/**
	 * arrayがnull、またはarray.lengthが0である場合にはtrueを返し、その以外の場合にはfalseを返す。(<b>Korean</
	 * b> : array가 null 또는 array.length가 0이면 true를 반환하고 아니면 false를 반환한다.)
	 * 
	 * @param array
	 * @return
	 */
	public static boolean isEmpty(Object[] array) {
		return array == null || array.length == 0;
	}

	/**
	 * arrayの長さを求む。(<b>Korean</b> : array의 길이를 구한다.)
	 * 
	 * @param array
	 * @return
	 */
	public static int getLength(Object[] array) {
		return array != null ? array.length : 0;
	}

	/**
	 * value1とvalue2の値を比較し、valiue1の方が大きい場合には正数、同じである場合には0、小さい場合には負数を返す。(<b>
	 * Korean</b> : value1과 value2의 값의 크기를 비교하여 value1이 크면 양수, 같으면 0, 작으면 음수를
	 * 반환한다.)
	 * 
	 * @param value1
	 * @param value2
	 * @return
	 */
	public static int compare(Object value1, Object value2) {
		if (isEmpty(value1) && isEmpty(value2)) {
			return 0;
		} else if (value1 != null && value2 == null) {
			return 1;
		} else if (value1 == null && value2 != null) {
			return -1;
		}
		return ((Comparable) value1).compareTo(value2);
	}

	/**
	 * value1とvalue2の値を比較し、valiue1の方が大きい場合には正数、同じである場合には0、小さい場合には負数を返す。(<b>
	 * Korean</b> : value1과 value2의 값의 크기를 비교하여 value1이 크면 양수, 같으면 0, 작으면 음수를
	 * 반환한다.)
	 * 
	 * @param value1
	 * @param value2
	 * @return
	 */
	public static int compare(Object[] values1, Object[] values2) {
		if (getLength(values1) != getLength(values2)) {
			return getLength(values1) - getLength(values2);
		}
		if (isEmpty(values1) && isEmpty(values2)) {
			return 0;
		} else if (!isEmpty(values1) && isEmpty(values2)) {
			return 1;
		} else if (isEmpty(values1) && !isEmpty(values2)) {
			return -1;
		}
		for (int i = 0; i < values1.length; i++) {
			int com = compare(values1[i], values2[i]);
			if (com != 0)
				return com;
		}
		return 0;
	}

	/**
	 * joinKeysをキーにleftRecordから条件(Conditions)を求める。(<b>Korean</b> : leftRecord에서
	 * joinKeys를 키로 사용해서 조건(Conditions)을 구한다.)
	 * 
	 * @param leftRecord
	 * @param joinKeys
	 * @return
	 */
	public static Conditions extractConditions(Map leftRecord, JoinKeys joinKeys) {
		if (joinKeys == null || joinKeys.size() == 0)
			return null;

		Conditions conditions = new Conditions();
		for (int k = 0; k < joinKeys.size(); k++) {
			String key = joinKeys.getRightKey(k);
			Object val = leftRecord != null ? leftRecord.get(joinKeys
					.getLeftKey(k)) : null;
			String originalOP = joinKeys.getOperator(k);
			String op = originalOP;
			if (Conditions.OP_STR_LESS.equals(originalOP))
				op = Conditions.OP_STR_GREATER;
			else if (Conditions.OP_STR_LESS_EQUAL.equals(originalOP))
				op = Conditions.OP_STR_GREATER_EQUAL;
			else if (Conditions.OP_STR_GREATER.equals(originalOP))
				op = Conditions.OP_STR_LESS;
			else if (Conditions.OP_STR_GREATER_EQUAL.equals(originalOP))
				op = Conditions.OP_STR_LESS_EQUAL;
			conditions.and(key, op, val);
		}
		if (joinKeys.isNonWildcard())
			conditions.setNonWildcard();
		else if (joinKeys.isLeftWildcard())
			conditions.setInnerWildcard();
		else if (joinKeys.isRightWildcard())
			conditions.setOuterWildcard();
		else if (joinKeys.isLeftRegExpr())
			conditions.setInnerRegExpr();
		else if (joinKeys.isRightRegExpr())
			conditions.setOuterRegExpr();

		return conditions;
	}

	/**
	 * keysをキーを用いてレコードから条件(Conditions)を求める。(<b>Korean</b> : record에서 keys를 키로
	 * 사용해서 조건(Conditions)을 구한다.)
	 * 
	 * @param record
	 * @param keys
	 * @return
	 */
	public static Conditions extractConditions(Map record, String[] keys) {
		if (keys == null || keys.length == 0)
			return null;

		Conditions conditions = new Conditions();
		for (int k = 0; k < keys.length; k++)
			conditions
					.and(keys[k], record != null ? record.get(keys[k]) : null);
		return conditions;
	}

	/**
	 * keysをキーにmapからEntryを抽出して新しいMapを作成し、返す。(<b>Korean</b> : map에서 keys를 키로한
	 * Entry들을 추출하여 새로운 Map을 만들어서 반환한다.)
	 * 
	 * @param map
	 * @param keys
	 * @return
	 */
	public static Map extractMap(Map map, String[] keys) {
		if (keys == null || keys.length == 0)
			return null;

		Map ret = new HashMap();
		for (int k = 0; k < keys.length; k++)
			ret.put(keys[k], map != null ? map.get(keys[k]) : null);
		return ret;
	}

	/**
	 * ログ記録のために使用する。Listを文字列に返す。(<b>Korean</b> : 로그 기록을 위해 사용한다. List를 문자열로
	 * 변환한다.)
	 * 
	 * @param list
	 * @return
	 */
	public static String toString(List<Map> list) {
		return toString(null, list, (String[]) null);
	}

	/**
	 * ログ記録のために使用する。Listを文字列に返す。(<b>Korean</b> : 로그 기록을 위해 사용한다. List를 문자열로
	 * 변환한다.)
	 * 
	 * @param msg
	 *            (<b>Korean</b> : 로그 메시지)
	 * @param list
	 * @return
	 */
	public static String toString(String msg, List<Map> list) {
		return toString(msg, list, (String[]) null);
	}

	/**
	 * ログ記録のために使用する。Listを文字列に返す。(<b>Korean</b> : 로그 기록을 위해 사용한다. List를 문자열로
	 * 변환한다.)
	 * 
	 * @param msg
	 *            ログメッセージ (<b>Korean</b> : 로그 메시지)
	 * @param list
	 * @param selectKeys
	 *            ログに記録するキーリスト (<b>Korean</b> : 로그에 기록할 키 목록)
	 * @return
	 */
	public static String toString(String msg, List<Map> list,
			List<String> selectKeys) {
		return toString(msg, list,
				selectKeys != null ? selectKeys.toArray(new String[] {}) : null);
	}

	/**
	 * ログ記録のために使用する。Listを文字列に返す。(<b>Korean</b> : 로그 기록을 위해 사용한다. List를 문자열로
	 * 변환한다.)
	 * 
	 * @param msg
	 *            ログメッセージ (<b>Korean</b> : 로그 메시지)
	 * @param list
	 * @param selectKeys
	 *            ログに記録するキーリスト (<b>Korean</b> : 로그에 기록할 키 목록)
	 * @return
	 */
	public static String toString(String msg, List<Map> list,
			String[] selectKeys) {
		StringBuilder sb = new StringBuilder();
		if (!StringUtil.isEmpty(msg))
			sb.append("# ").append(msg).append(":").append(CommonUtil.NEW_LINE);
		if (list != null) {
			for (int r = 0; r < list.size(); r++) {
				sb.append("[").append(r).append("] ");
				Map row = list.get(r);
				if (selectKeys == null || selectKeys.length == 0)
					sb.append(row);
				else
					sb.append(cloneRecord(row, selectKeys));
				sb.append(CommonUtil.NEW_LINE);
			}
			sb.append(CommonUtil.NEW_LINE);
		}
		return sb.toString();
	}

	/**
	 * ログ記録のために使用する。Listを文字列に返す。 (<b>Korean</b> : 로그 기록을 위해 사용한다. 레코드를 문자열로
	 * 변환한다.)
	 * 
	 * @param msg
	 *            ログメッセージ (<b>Korean</b> : 로그 메시지)
	 * @param record
	 * @return
	 */
	public static String toString(String msg, Map record) {
		return toString(msg, record, (String[]) null);
	}

	/**
	 * ログ記録のために使用する。Listを文字列に返す。(<b>Korean</b> : 로그 기록을 위해 사용한다. 레코드를 문자열로
	 * 변환한다.)
	 * 
	 * @param msg
	 *            ログメッセージ (<b>Korean</b> : 로그 메시지)
	 * @param record
	 * @param selectKeys
	 *            ログに記録するリスト (<b>Korean</b> : 로그에 기록할 키 목록)
	 * @return
	 */
	public static String toString(String msg, Map record, String[] selectKeys) {
		StringBuilder sb = new StringBuilder();
		if (!StringUtil.isEmpty(msg))
			sb.append("# ").append(msg).append(":");
		if (selectKeys == null || selectKeys.length == 0)
			sb.append(record);
		else
			sb.append(cloneRecord(record, selectKeys));
		return sb.toString();
	}

	/**
	 * レコードをコピーする。 (<b>Korean</b> : 레코드를 복제한다.)
	 * 
	 * @param record
	 * @return
	 */
	public static Map cloneRecord(final Map record) {
		if (record == null)
			return null;
		Map ret = new HashMap();
		Iterator it = record.entrySet().iterator();
		while (it.hasNext()) {
			Entry entry = (Entry) it.next();
			ret.put(entry.getKey(), entry.getValue());
		}
		return ret;
	}

	/**
	 * 与えられたkeysカラムのみ抽出し、新しいレコードを作成し、返す。 <b>(Korean :</b> 주어진 keys 칼럼들만 추출하여 새로운 레코드를 만들어 반환한다.<b>)</b>
	 * 
	 * @param record
	 * @param keys
	 *            コピーするカラムのkey <b>(Korean :</b> 복제할 컬럼들의 key<b>)</b>
	 * @return
	 */
	public static Map cloneRecord(Map record, String[] keys) {
		return cloneRecord(record, null, keys);
	}

	/**
	 * 与えられたkeysカラムのみ抽出し、新しいレコードを作成し、返す。但し、返されるレコードのキー名にprefixが付ける。 <b>(Korean :</b> 주어진 keys 칼럼들만 추출하여 새로운 레코드를 만들어 반환한다. 단, 반환되는 레코드의
	 * 키 이름엔 prefix가 붙는다.<b>)</b>
	 * 
	 * @param map
	 * @param prefix
	 *            新しく生成されるカラムのキーに付けるprefix <b>(Korean :</b> 새로 생성될 레코드의 칼럼의 key에 붙을 prefix<b>)</b>
	 * @param keys
	 *            コピーするカラムのキー <b>(Korean :</b> 복제할 컬럼들의 key<b>)</b>
	 * @return
	 */
	public static Map cloneRecord(Map map, String prefix, String[] keys) {
		if (map == null)
			return null;
		Map ret = new HashMap();
		if (StringUtil.isEmpty(prefix)) {
			if (keys == null || keys.length == 0) {
				Iterator it = map.entrySet().iterator();
				while (it.hasNext()) {
					Entry entry = (Entry) it.next();
					ret.put(entry.getKey(), entry.getValue());
				}
			} else {
				for (int k = 0; k < keys.length; k++) {
					ret.put(keys[k], map.get(keys[k]));
				}
			}
		} else {
			if (keys == null || keys.length == 0) {
				Iterator it = map.entrySet().iterator();
				while (it.hasNext()) {
					Entry entry = (Entry) it.next();
					ret.put(prefix + entry.getKey(), entry.getValue());
				}
			} else {
				for (int k = 0; k < keys.length; k++) {
					ret.put(prefix + keys[k], map.get(keys[k]));
				}
			}
		}
		return ret;
	}

	/**
	 * List<Map>をコピーする。(<b>Korean</b> : List&lt;Map&gt;을 복제한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @return
	 */
	public static List<Map> cloneList(final List<Map> list) {
		if (list == null)
			return null;
		List<Map> ret = new ArrayList<Map>();
		for (int r = 0; r < list.size(); r++)
			ret.add(cloneRecord(list.get(r)));
		return ret;
	}

	/**
	 * leftRecordとrightRecordを結合したレコードを返す。返されたレコード修正しても、原本のレコードは修正されない。
	 * (<b>Korean</b> : leftRecord와 rightRecord를 병합한 레코드를 반환한다. 반환된 레코드를 수정하여도
	 * 원본 레코드는 수정되지 않는다.)
	 * 
	 * @param leftRecord
	 *            レコード (<b>Korean</b> : 레코드)
	 * @param leftSelectKeys
	 *            leftRecordから抽出するカラムキー (<b>Korean</b> : leftRecord에서 추출할 칼럼 키)
	 * @param rightRecord
	 *            レコード (<b>Korean</b> : 레코드)
	 * @param rightSelectKeys
	 *            rightRecordから抽出するカラムキー (<b>Korean</b> : rightRecord에서 추출할 칼럼
	 *            키)
	 * @return
	 */
	public static Map merge(Map leftRecord, String[] leftSelectKeys,
			Map rightRecord, String[] rightSelectKeys) {
		return merge(leftRecord, null, leftSelectKeys, rightRecord, null,
				rightSelectKeys);
	}

	private static boolean contains(Object[] arr, Object element) {
		if (arr == null || arr.length == 0)
			return false;
		for (int a = 0; a < arr.length; a++) {
			if (element.equals(arr[a]))
				return true;
		}
		return false;
	}

	/**
	 * leftRecordとrightRecordを結合したレコードを返す。返されたレコードを修正しても、原本のレコードは修正されない。
	 * (<b>Korean</b> : leftRecord와 rightRecord를 병합한 레코드를 반환한다. 반환된 레코드를 수정하여도
	 * 원본 레코드는 수정되지 않는다.)
	 * 
	 * @param leftRecord
	 *            レコード (<b>Korean</b> : 레코드)
	 * @param leftPrefix
	 *            カラムキーの前に付ける (<b>Korean</b> : 칼럼 키의 앞에 붙일 prefix. Ex.
	 *            leftPrefix.Column_Key)
	 * @param leftSelectKeys
	 *            leftRecordから抽出するカラムキー (<b>Korean</b> : leftRecord에서 추출할 칼럼 키)
	 * @param rightRecord
	 *            レコード (<b>Korean</b> : 레코드)
	 * @param rightPrefix
	 *            カラムキーの前に付ける (<b>Korean</b> : 칼럼 키의 앞에 붙일 prefix. Ex.
	 *            rightPrefix.Column_Key)
	 * @param rightSelectKeys
	 *            rightRecordから抽出するカラムキー (<b>Korean</b> : rightRecord에서 추출할 칼럼
	 *            키)
	 * @return
	 */
	public static Map merge(Map leftRecord, String leftPrefix,
			String[] leftSelectKeys, Map rightRecord, String rightPrefix,
			String[] rightSelectKeys) {
		Map merged = null;

		if (leftRecord != null && leftRecord.size() > 0) {
			merged = cloneRecord(leftRecord, leftPrefix, leftSelectKeys);
		}

		if (rightRecord != null && rightRecord.size() > 0) {
			Iterator it = rightRecord.entrySet().iterator();
			while (it.hasNext()) {
				Entry entry = (Entry) it.next();
				String key = (String) entry.getKey();
				Object value = entry.getValue();
				if (rightSelectKeys == null || rightSelectKeys.length == 0
						|| contains(rightSelectKeys, key)) {
					if (merged == null)
						merged = new HashMap();
					if (StringUtil.isEmpty(rightPrefix)) {
						if (!merged.containsKey(key))
							merged.put(key, value);
						merged.put("RIGHT." + key, value);
					} else {
						merged.put(rightPrefix + "." + key, value);
					}
				}
			}
		}

		return merged;
	}
}
