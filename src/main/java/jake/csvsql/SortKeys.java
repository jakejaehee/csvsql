package jake.csvsql;

/**
 * ソーティングに使用するキーを設定する。(<b>Korean</b> : 정렬에 사용할 키를 설정한다.)
 * 
 * @author Jake Lee
 */
public class SortKeys extends AbstractQueryKeys {

	public SortKeys() {
	}

	/**
	 * ソーティングに使用するキーを設定する。 (<b>Korean</b> : 정렬에 사용할 키를 설정한다.)
	 * 
	 * @param key
	 */
	public SortKeys(String key) {
		super(key);
	}

	/**
	 * ソーティングに使用するキーとソート方式を設定する。(<b>Korean</b> : 정렬에 사용할 키와 정렬방식을 설정한다.)
	 * 
	 * @param key
	 *            ソーティングに使用するキー (<b>Korean</b> : 정렬에 사용할 키)
	 * @param ascendSort
	 *            trueの場合は昇順にソートし、falseの場合には降順にソートする。(<b>Korean</b> : true이면
	 *            오름차순 정렬, false이면 내림차순 정렬을 한다.)
	 */
	public SortKeys(String key, boolean ascendSort) {
		super(key, ascendSort);
	}

	/**
	 * ソーティングに使用するキーを設定する。 (<b>Korean</b> : 정렬에 사용할 키를 설정한다.)
	 * 
	 * @param key
	 *            ソーティングに使用するキー (<b>Korean</b> : 정렬에 사용할 키)
	 */
	public SortKeys and(String key) {
		return and(key, true);
	}

	/**
	 * ソーティングに使用するキーとソート方式を設定する。 (<b>Korean</b> : 정렬에 사용할 키와 정렬방식을 설정한다.)
	 * 
	 * @param key
	 *            ソーティングに使用するキー (<b>Korean</b> : 정렬에 사용할 키)
	 * @param ascendSort
	 *            trueの場合は昇順にソートし、falseの場合には降順にソートする。 (<b>Korean</b> : true이면
	 *            오름차순 정렬, false이면 내림차순 정렬을 한다.)
	 */
	public SortKeys and(String key, boolean ascendSort) {
		add(new KeyAndSort(key, ascendSort));
		return this;
	}
}
