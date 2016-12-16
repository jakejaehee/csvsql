package jake.csvsql;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.jdi.scm.util.SCMUtil;

/**
 * List<Map>から任意のレコードを検索するための条件値として使用される。
 * 例としてSQL文のwhere句のような役割をする。(<b>Korean</b> : List<Map>에서 임의의 레코드를 검색하기 위한 조건 값으로
 * 사용된다. 마치 SQL문의 where 구문과 같은 역할을 한다.)
 * 
 * @author Jake Lee
 * 
 */
public class Conditions {
	private static final Logger LOG = Logger.getLogger(Conditions.class);

	/**
	 * <code>
	 * Default
	 * To not use wildcard matching
	 * </code>
	 */
	public static final short NON_WILDCARD = 0;

	/**
	 * To use wildcard pattern on the inside of conditions.
	 */
	public static final short INNER_WILDCARD = 1;

	/**
	 * To use wildcard pattern on the outside of conditions.
	 */
	public static final short OUTER_WILDCARD = 2;

	/**
	 * To use regular expression pattern on the inside of conditions.
	 */
	public static final short INNER_REG_EXPR = 3;

	/**
	 * To use regular expression pattern on the outside of conditions.
	 */
	public static final short OUTER_REG_EXPR = 4;

	private short wildcardType = NON_WILDCARD;

	private static final short OP_EQUAL = 0;
	private static final short OP_LESS = 1;
	private static final short OP_LESS_EQUAL = 2;
	private static final short OP_GREATER = 3;
	private static final short OP_GREATER_EQUAL = 4;

	/**
	 * 演算子 '=' (<b>Korean</b> : 연산자 '=')
	 */
	public static final String OP_STR_EQUAL = "=";

	/**
	 * 演算子 '=='. '='と同一。(<b>Korean</b> : 연산자 '=='. '='와 동일하다.)
	 */
	public static final String OP_STR_EQUAL2 = "==";

/**
	 * 演算子 '<'(<b>Korean</b> : 연산자 '<')
	 */
	public static final String OP_STR_LESS = "<";

	/**
	 * 演算子 '<='(<b>Korean</b> : 연산자 '<=')
	 */
	public static final String OP_STR_LESS_EQUAL = "<=";

	/**
	 * 演算子 '>' (<b>Korean</b> : 연산자 '>')
	 */
	public static final String OP_STR_GREATER = ">";

	/**
	 * 演算子 '>=' (<b>Korean</b> : 연산자 '>=')
	 */
	public static final String OP_STR_GREATER_EQUAL = ">=";

	/**
	 * 演算子 '!=' (<b>Korean</b> : 연산자 '!=')
	 */
	public static final String OP_STR_NOT_EQUAL = "!=";

	private static final Map<String, Short> opMap1 = new HashMap<String, Short>();
	static {
		opMap1.put(OP_STR_EQUAL, OP_EQUAL);
		opMap1.put(OP_STR_EQUAL2, OP_EQUAL);
		opMap1.put(OP_STR_LESS, OP_LESS);
		opMap1.put(OP_STR_LESS_EQUAL, OP_LESS_EQUAL);
		opMap1.put(OP_STR_GREATER, OP_GREATER);
		opMap1.put(OP_STR_GREATER_EQUAL, OP_GREATER_EQUAL);
	};

	private static final Map<Short, String> opMap2 = new HashMap<Short, String>();
	static {
		opMap2.put(OP_EQUAL, OP_STR_EQUAL);
		opMap2.put(OP_LESS, OP_STR_LESS);
		opMap2.put(OP_LESS_EQUAL, OP_STR_LESS_EQUAL);
		opMap2.put(OP_GREATER, OP_STR_GREATER);
		opMap2.put(OP_GREATER_EQUAL, OP_STR_GREATER_EQUAL);
	};

	private Criteria[] isCri = new Criteria[10];
	private int isCriIdx = 0;

	private Criteria[] notCri = new Criteria[10];
	private int notCriIdx = 0;

	class Criteria {
		final public String key;
		final public short op;
		final public Object val;

		public Criteria(String key, Object val) {
			this(key, OP_EQUAL, val);
		}

		public Criteria(String key, short op, Object val) {
			this.key = key;
			this.op = op;
			this.val = val;
		}

		public String toString() {
			return key + opMap2.get(op) + val;
		}
	}

	public Conditions() {
	}

	/**
	 * カラムキーと値を設定する。(<b>Korean</b> : 칼럼 키와 값을 설정한다.)
	 * 
	 * @param key
	 * @param value
	 */
	public Conditions(String key, Object value) {
		and(key, value);
	}

	/**
	 * <code>
	 * カラムキー、 比較演算子、 値を設定する。(<b>Korean</b> : 칼럼 키, 비교연산자, 값을 설정한다.)
	 * 
	 * op: =, <, <=, >, >=, !=
	 * Usage: Usage: new Conditions("CNT", "<=", "78");
	 * </code>
	 * 
	 * @param key
	 *            칼럼 키
	 * @param op
	 *            비교 연산자
	 * @param value
	 *            값
	 */
	public Conditions(String key, String op, Object value) {
		and(key, op, value);
	}

	/**
	 * 比較演算子 '='に該当するカラムキーと値を含むMap型で一括設定を行う。(<b>Korean</b> : 연산자 '='에 해당하는 칼럼 키와
	 * 값을 담은 Map으로 일괄 설정한다.)
	 * 
	 * @param positiveConditions
	 */
	public Conditions(Map positiveConditions) {
		Iterator it = positiveConditions.entrySet().iterator();
		while (it.hasNext()) {
			Entry entry = (Entry) it.next();
			String key = (String) entry.getKey();
			Object val = entry.getValue();
			putIsCri(new Criteria(key, val));
		}
	}

	private void putIsCri(Criteria cri) {
		if (isCri.length <= isCriIdx) {
			Criteria[] tmp = new Criteria[isCri.length + 10];
			System.arraycopy(isCri, 0, tmp, 0, isCri.length);
			isCri = tmp;
		}
		isCri[isCriIdx++] = cri;
	}

	private void putNotCri(Criteria cri) {
		if (notCri.length <= notCriIdx) {
			Criteria[] tmp = new Criteria[notCri.length + 10];
			System.arraycopy(notCri, 0, tmp, 0, notCri.length);
			notCri = tmp;
		}
		notCri[notCriIdx++] = cri;
	}

	/**
	 * 与えられた条件(conditions)で一括追加する。(<b>Korean</b> : 주어진 조건(conditions)로 일괄 추가한다.)
	 * 
	 * @param conditions
	 */
	public void setAll(Conditions conditions) {
		this.isCri = conditions.isCri;
		this.isCriIdx = conditions.isCriIdx;
		this.notCri = conditions.notCri;
		this.notCriIdx = conditions.notCriIdx;
		this.wildcardType = conditions.wildcardType;
	}

	/**
	 * Wildcard Matchingを使用しないように設定する。(<b>Korean</b> : Wildcard Matching을 사용하지
	 * 않도록 설정한다.)
	 */
	public Conditions setNonWildcard() {
		this.wildcardType = NON_WILDCARD;
		return this;
	}

	/**
	 * 条件となる値を範囲でWildcardパターンを使用するように設定する。これがデフォルトである。 (<b>Korean</b> : 조건 값 내에서
	 * Wildcard 패턴을 사용하도록 설정한다. 이것이 default이다.)
	 */
	public Conditions setInnerWildcard() {
		this.wildcardType = INNER_WILDCARD;
		return this;
	}

	/**
	 * 条件値ではWildcardパターンを使用せず、比較の対象となるレコードにおいてパターンが使用されるように設定する。(<b>Korean</b> :
	 * 조건 값에서 Wildcard 패턴을 사용하지 않고 비교의 대상이 되는 레코드에서 패턴이 사용되도록 설정한다.)
	 */
	public Conditions setOuterWildcard() {
		this.wildcardType = OUTER_WILDCARD;
		return this;
	}

	/**
	 * 条件となる値を範囲でRegular Expressionパターンを使用するように設定する。これがデフォルトである。 (<b>Korean</b>
	 * : 조건 값 내에서 Regular Expression 패턴을 사용하도록 설정한다. 이것이 default이다.)
	 */
	public Conditions setInnerRegExpr() {
		this.wildcardType = INNER_REG_EXPR;
		return this;
	}

	/**
	 * 条件値ではRegular
	 * Expressionパターンを使用せず、比較の対象となるレコードにおいてパターンが使用されるように設定する。(<b>Korean</b> : 조건
	 * 값에서 Regular Expression 패턴을 사용하지 않고 비교의 대상이 되는 레코드에서 패턴이 사용되도록 설정한다.)
	 */
	public Conditions setOuterRegExpr() {
		this.wildcardType = OUTER_REG_EXPR;
		return this;
	}

	/**
	 * 条件となるキーと値を追加する。比較演算子は '='になる。(<b>Korean</b> : 조건 키와 값을 추가한다. 비교연산자는 '='가
	 * 된다.)
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public Conditions and(String key, Object value) {
		return and(key, OP_STR_EQUAL, value);
	}

	/**
	 * 条件キー、比較演算子、値を設定する。(<b>Korean</b> : 조건 키, 비교 연산자, 값을 설정한다.)
	 * 
	 * @param key
	 * @param op
	 *            =, <, <=, >, >=, !=
	 * @param value
	 * @return
	 */
	public Conditions and(String key, String op, Object value) {
		if ("!=".equals(op) || "<>".equals(op)) {
			putNotCri(new Criteria(key, OP_EQUAL, value));
		} else {
			putIsCri(new Criteria(key, opMap1.get(op), value));
		}
		return this;
	}

	/**
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public Conditions andNot(String key, Object value) {
		return and(key, OP_STR_NOT_EQUAL, value);
	}

	private String toString(Criteria[] cri, int len) {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		for (int c = 0; c < len; c++) {
			if (c > 0)
				sb.append(" and ");
			sb.append(cri[c]);
		}
		sb.append(")");
		return sb.toString();
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (isCriIdx > 0)
			sb.append(toString(isCri, isCriIdx));
		if (notCriIdx > 0) {
			if (sb.length() > 0)
				sb.append(" and ");
			sb.append(toString(notCri, notCriIdx));
		}
		return sb.toString();
	}

	/**
	 * 与えられたレコードが条件に適しているか確認する。(<b>Korean</b> : 주어진 레코드가 조건에 맞는지 검사한다.)
	 * 
	 * @param record
	 * @return
	 */
	public boolean suits(final Map record) {
		if (isCriIdx > 0) {
			for (int c = 0; c < isCriIdx; c++) {
				Criteria criteria = isCri[c];
				Object conditionVal = criteria.val;
				Object targetVal = record.get(criteria.key);

				if (wildcardType == NON_WILDCARD && criteria.op == OP_EQUAL) {
					if ((!SCMUtil.isEmpty(conditionVal) && SCMUtil
							.isEmpty(targetVal))
							|| (SCMUtil.isEmpty(conditionVal) && !SCMUtil
									.isEmpty(targetVal))) {
						return false;
					} else if (conditionVal != null
							&& !conditionVal.equals(targetVal)) {
						return false;
					}
				} else {
					if (!match(conditionVal, targetVal, criteria))
						return false;
				}
			}
		}
		if (notCriIdx > 0) {
			for (int c = 0; c < notCriIdx; c++) {
				Criteria criteria = notCri[c];
				Object conditionVal = criteria.val;
				Object targetVal = record.get(criteria.key);

				if (wildcardType == NON_WILDCARD && criteria.op == OP_EQUAL) {
					if (SCMUtil.isEmpty(conditionVal)
							&& SCMUtil.isEmpty(targetVal)) {
						return false;
					} else if (conditionVal != null
							&& conditionVal.equals(targetVal)) {
						return false;
					}
				} else {
					if (match(conditionVal, targetVal, criteria))
						return false;
				}
			}
		}
		return true;
	}

	private boolean match(Object conditionVal, Object targetVal,
			Criteria criteria) {
		if (SCMUtil.isEmpty(conditionVal) && SCMUtil.isEmpty(targetVal)) {
			if (criteria.op == OP_LESS) {
				return false;
			} else if (criteria.op == OP_GREATER) {
				return false;
			} else {
				return true;
			}
		} else if (conditionVal instanceof String
				&& targetVal instanceof String) {
			if (wildcardType == NON_WILDCARD) {
				if (criteria.op == OP_EQUAL) {
					return conditionVal != null
							&& conditionVal.equals(targetVal);
				} else if (criteria.op == OP_LESS) {
					return ListSqlUtil.compare(conditionVal, targetVal) > 0;
				} else if (criteria.op == OP_LESS_EQUAL) {
					return ListSqlUtil.compare(conditionVal, targetVal) >= 0;
				} else if (criteria.op == OP_GREATER) {
					return ListSqlUtil.compare(conditionVal, targetVal) < 0;
				} else if (criteria.op == OP_GREATER_EQUAL) {
					return ListSqlUtil.compare(conditionVal, targetVal) <= 0;
				}
			} else if (wildcardType == INNER_WILDCARD) {
				return SCMUtil.wildCardMatch((String) targetVal,
						(String) conditionVal);
			} else if (wildcardType == OUTER_WILDCARD) {
				return SCMUtil.wildCardMatch((String) conditionVal,
						(String) targetVal);
			} else if (wildcardType == INNER_REG_EXPR) {
				return SCMUtil.regExprMatch((String) targetVal,
						(String) conditionVal);
			} else if (wildcardType == OUTER_REG_EXPR) {
				return SCMUtil.regExprMatch((String) conditionVal,
						(String) targetVal);
			}
		} else {
			if (criteria.op == OP_EQUAL) {
				return ListSqlUtil.compare(conditionVal, targetVal) == 0;
			} else if (criteria.op == OP_LESS) {
				return ListSqlUtil.compare(conditionVal, targetVal) > 0;
			} else if (criteria.op == OP_LESS_EQUAL) {
				return ListSqlUtil.compare(conditionVal, targetVal) >= 0;
			} else if (criteria.op == OP_GREATER) {
				return ListSqlUtil.compare(conditionVal, targetVal) < 0;
			} else if (criteria.op == OP_GREATER_EQUAL) {
				return ListSqlUtil.compare(conditionVal, targetVal) <= 0;
			}
		}
		return false;
	}

	private boolean isEqualsOnlyCondition() {
		if (notCriIdx > 0)
			return false;
		for (int c = 0; c < isCriIdx; c++) {
			if (isCri[c].op != OP_EQUAL)
				return false;
		}
		return true;
	}

	private String[] getEqualKeys() {
		String[] keys = new String[isCriIdx];
		for (int c = 0; c < isCriIdx; c++) {
			keys[c] = isCri[c].key;
		}
		return keys;
	}

	protected String[] getEqualValues() {
		String[] keys = new String[isCriIdx];
		for (int c = 0; c < isCriIdx; c++) {
			keys[c] = String.valueOf(isCri[c].val);
		}
		return keys;
	}
}
