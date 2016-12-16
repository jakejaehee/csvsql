package jake.csvsql;

/**
 * <code>
 * 取得するキーを設定する。(SQL文のSELECT句において取得するカラムを指定することと同様である。) (<b>Korean</b> : 취득할 칼럼 키를 설정한다(SQL문의 SELECT 구문에서 취득할 칼럼을 지정하는 것과 같다.))
 * <p>
 * o SQL文のselect句の例示: select col_a, col_b, col_c from table_a (<b>Korean</b> : SQL문의 SELECT 구문 예시: select col_a, col_b, col_c from table_a)
 * </code>
 * 
 * @author Jake Lee
 * 
 */
public class SelectKeys extends AbstractQueryKeys {

	public SelectKeys() {
	}

	/**
	 * 取得するキーを設定する。 (<b>Korean</b> : 취득할 키를 설정한다.)
	 * 
	 * @param key
	 */
	public SelectKeys(String key) {
		super(key);
	}

	/**
	 * 取得するキーとソート方式を設定する。(<b>Korean</b> : 취득할 키와 정렬방식을 설정한다.)
	 * 
	 * @param key
	 * @param ascendSort
	 *            trueの場合は昇順にソートし、falseの場合には降順にソートする (<b>Korean</b> : true이면
	 *            오름차순 정렬, false이면 내림차순 정렬이 된다.)
	 */
	public SelectKeys(String key, boolean ascendSort) {
		super(key, ascendSort);
	}

	/**
	 * 取得するキーを設定する。 (<b>Korean</b> : 취득할 키를 설정한다.)
	 */
	public SelectKeys and(String key) {
		return and(key, true);
	}

	/**
	 * 取得するキーを設定する。 (<b>Korean</b> : 취득할 키와 정렬방식을 설정한다.)
	 * 
	 * @param key
	 * @param ascendSort
	 *            trueの場合は昇順にソートし、falseの場合には降順にソートする。 (<b>Korean</b> : true이면
	 *            오름차순 정렬, false이면 내림차순 정렬이 된다.)
	 */
	public SelectKeys and(String key, boolean ascendSort) {
		add(new KeyAndSort(key, ascendSort));
		return this;
	}

	/**
	 * ソートキーに転換 (<b>Korean</b> : 정렬 키로 전환)
	 */
	public SortKeys toSortKeys() {
		SortKeys sortKeys = new SortKeys();
		for (int k = 0; k < this.size(); k++)
			sortKeys.add(this.get(k));
		return sortKeys;
	}
}
