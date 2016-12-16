package jake.csvsql;

import java.util.ArrayList;
import java.util.List;

/**
 * JOINに使用するキー(<b>Korean</b> : JOIN에 사용할 키)
 * 
 * @author Jake Lee
 * 
 */
public class JoinKeys {
	private List<String[]> keys = new ArrayList<String[]>();
	private short wildcardType = Conditions.NON_WILDCARD;
	private boolean allEqualOPs = true;

	protected boolean isIndexEnabled() {
		return allEqualOPs && wildcardType == Conditions.NON_WILDCARD;
	}

	public JoinKeys() {
	}

	/**
	 * JOINに使用するキーを設定する。但し、JOINの対象となる両List<Map>の該当カラムキーは同一であることを前提とする。(<b>Korean
	 * </b> : JOIN에 사용할 키를 설정한다. 단, JOIN의 대상이 되는 두 List<Map>의 해당 칼럼 키는 동일하다는 것을
	 * 전재로 한다.)
	 * 
	 * @param keys
	 */
	public JoinKeys(String[] keys) {
		for (int k = 0; keys != null && k < keys.length; k++) {
			and(keys[k]);
		}
	}

	/**
	 * JOINに使用するキーを設定する。但し、JOINの対象となる両List<Map>の該当カラムキーは同一であることを前提とする。(<b>Korean
	 * </b> : JOIN에 사용할 키를 설정한다. 단, JOIN의 대상이 되는 두 List<Map>의 해당 칼럼 키는 동일하다는 것을
	 * 전재로 한다.)
	 * 
	 * @param key
	 */
	public JoinKeys(String key) {
		and(key);
	}

	/**
	 * JOINに使用するキーを設定する。両List<Map>のキーが異なる場合に使用する。(<b>Korean</b> : JOIN에 사용할 키를
	 * 설정한다. 두 List<Map>의 키가 다를 경우에 사용한다.)
	 * 
	 * @param leftKey
	 *            左List<Map>のカラムキー (<b>Korean</b> : 왼쪽 List<Map>의 칼럼 키)
	 * @param rightKey
	 *            右List<Map>のカラムキー (<b>Korean</b> : 오른쪽 List<Map>의 칼럼 키)
	 */
	public JoinKeys(String leftKey, String rightKey) {
		and(leftKey, rightKey);
	}

	/**
	 * コンストラクタ。JOINに使用するキーを設定する。<b>(Korean :</b> 생성자. JOIN에 사용할 키를 설정한다.<b>)</b>
	 * 
	 * @param leftKey
	 *            左List<Map>のカラムキー (<b>Korean</b> : 왼쪽 List<Map>의 칼럼 키)
	 * @param op
	 *            演算子 <b>(Korean :</b> 연산자<b>)</b> Examples:
	 *            {@link Conditions#OP_STR_EQUAL},
	 *            {@link Conditions#OP_STR_GREATER} ...
	 * @param rightKey
	 *            右List<Map>のカラムキー (<b>Korean</b> : 오른쪽 List<Map>의 칼럼 키)
	 */
	public JoinKeys(String leftKey, String op, String rightKey) {
		and(leftKey, op, rightKey);
	}

	/**
	 * JOINに使用するキーを設定する。但し、JOINの対象となる両List<Map>の該当カラムキーは同一であることを前提とする。(<b>Korean
	 * </b> : JOIN에 사용할 키를 설정한다. 단, JOIN의 대상이 되는 두 List<Map>의 해당 칼럼 키는 동일하다는 것을
	 * 전재로 한다.)
	 * 
	 * @param key
	 * @return
	 */
	public JoinKeys and(String key) {
		return and(key, Conditions.OP_STR_EQUAL, key);
	}

	/**
	 * JOINに使用するキーを設定する。両List<Map>のキーが異なる場合に使用する。(<b>Korean</b> : JOIN에 사용할 키를
	 * 설정한다. 두 List<Map>의 키가 다를 경우에 사용한다.)
	 * 
	 * @param leftKey
	 *            (<b>Korean</b> : 왼쪽 List<Map>의 칼럼 키)
	 * @param rightKey
	 *            (<b>Korean</b> : 오른쪽 List<Map>의 칼럼 키)
	 * @return
	 */
	public JoinKeys and(String leftKey, String rightKey) {
		return and(leftKey, Conditions.OP_STR_EQUAL, rightKey);
	}

	/**
	 * JOIN キー追加 <b>(Korean :</b> JOIN 키 추가<b>)</b>
	 * 
	 * @param leftKey
	 *            左List<Map>のカラムキー (<b>Korean</b> : 왼쪽 List<Map>의 칼럼 키)
	 * @param op
	 *            演算子 <b>(Korean :</b> 연산자<b>)</b> Examples:
	 *            {@link Conditions#OP_STR_EQUAL},
	 *            {@link Conditions#OP_STR_GREATER} ...
	 * @param rightKey
	 *            右List<Map>のカラムキー (<b>Korean</b> : 오른쪽 List<Map>의 칼럼 키)
	 * @return
	 */
	public JoinKeys and(String leftKey, String op, String rightKey) {
		if (!Conditions.OP_STR_EQUAL.equals(op)
				&& !Conditions.OP_STR_EQUAL2.equals(op))
			allEqualOPs = false;
		keys.add(new String[] { leftKey, op, rightKey });
		return this;
	}

	/**
	 * Wildcard Matchingを使用しないように設定する。(<b>Korean</b> : Wildcard Matching을 사용하지
	 * 않도록 설정한다.)
	 */
	public JoinKeys setNonWildcard() {
		this.wildcardType = Conditions.NON_WILDCARD;
		return this;
	}

	/**
	 * 条件となる値を範囲でWildcardパターンを使用するように設定する。これがデフォルトである。 (<b>Korean</b> : 조건 값 내에서
	 * Wildcard 패턴을 사용하도록 설정한다. 이것이 default이다.)
	 */
	public JoinKeys setLeftWildcard() {
		this.wildcardType = Conditions.INNER_WILDCARD;
		return this;
	}

	/**
	 * 条件値ではWildcardパターンを使用せず、比較の対象となるレコードにおいてパターンが使用されるように設定する。(<b>Korean</b> :
	 * 조건 값에서 Wildcard 패턴을 사용하지 않고 비교의 대상이 되는 레코드에서 패턴이 사용되도록 설정한다.)
	 */
	public JoinKeys setRightWildcard() {
		this.wildcardType = Conditions.OUTER_WILDCARD;
		return this;
	}

	/**
	 * 条件となる値を範囲でRegular Expressionパターンを使用するように設定する。これがデフォルトである。 (<b>Korean</b>
	 * : 조건 값 내에서 Regular Expression 패턴을 사용하도록 설정한다. 이것이 default이다.)
	 */
	public JoinKeys setLeftRegExpr() {
		this.wildcardType = Conditions.INNER_REG_EXPR;
		return this;
	}

	/**
	 * 条件値ではRegular
	 * Expressionパターンを使用せず、比較の対象となるレコードにおいてパターンが使用されるように設定する。(<b>Korean</b> : 조건
	 * 값에서 Regular Expression 패턴을 사용하지 않고 비교의 대상이 되는 레코드에서 패턴이 사용되도록 설정한다.)
	 */
	public JoinKeys setRightRegExpr() {
		this.wildcardType = Conditions.OUTER_REG_EXPR;
		return this;
	}

	protected boolean isNonWildcard() {
		return this.wildcardType == Conditions.NON_WILDCARD;
	}

	protected boolean isLeftWildcard() {
		return this.wildcardType == Conditions.INNER_WILDCARD;
	}

	protected boolean isRightWildcard() {
		return this.wildcardType == Conditions.OUTER_WILDCARD;
	}

	protected boolean isLeftRegExpr() {
		return this.wildcardType == Conditions.INNER_REG_EXPR;
	}

	protected boolean isRightRegExpr() {
		return this.wildcardType == Conditions.OUTER_REG_EXPR;
	}

	/**
	 * キーの数(<b>Korean</b> : 키의 갯수)
	 * 
	 * @return
	 */
	public int size() {
		return keys.size();
	}

	/**
	 * 与えられたインデックスに該当する左List<Map>のキーを返却する。(<b>Korean</b> : 주어진 인덱스에 해당하는 왼쪽
	 * List<Map>의 키를 반환한다.)
	 * 
	 * @param idx
	 *            設定されたキーの順に0から1ずつカウントアップする。(<b>Korean</b> : 설정된 키의 순으로 0부터 1씩
	 *            증가한다.)
	 * @return
	 */
	public String getLeftKey(int idx) {
		return idx >= 0 && idx < keys.size() ? keys.get(idx)[0] : null;
	}

	/**
	 * 与えられたインデックスに該当する右List<Map>のキーを返却する。(<b>Korean</b> : 주어진 인덱스에 해당하는 오른쪽
	 * List<Map>의 키를 반환한다.)
	 * 
	 * @param idx
	 *            設定されたキーの順に0から1ずつカウントアップする。(<b>Korean</b> : 설정된 키의 순으로 0부터 1씩
	 *            증가한다.)
	 * @return
	 */
	public String getRightKey(int idx) {
		return idx >= 0 && idx < keys.size() ? keys.get(idx)[2] : null;
	}

	protected String getOperator(int idx) {
		return idx >= 0 && idx < keys.size() ? keys.get(idx)[1] : null;
	}

	protected String[] getRightKeys() {
		String[] keys = new String[size()];
		for (int c = 0; c < keys.length; c++) {
			keys[c] = getRightKey(c);
		}
		return keys;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int k = 0; k < keys.size(); k++) {
			if (k > 0)
				sb.append(" and ");
			sb.append(getLeftKey(k)).append(Conditions.OP_STR_EQUAL)
					.append(getRightKey(k));
		}
		return sb.toString();
	}
}
