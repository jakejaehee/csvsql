package jake.csvsql;

/**
 * カラムキーとソート方式を設定する。(<b>Korean</b> : 칼럼 키와 정렬 방식을 설정한다.)
 * 
 * @author Jake Lee
 * 
 */
public class KeyAndSort {
	private final String key;
	private final boolean ascendSort;

	/**
	 * カラムキーとソート方式を設定する。(<b>Korean</b> : 칼럼 키와 정렬 방식을 설정한다.)
	 * 
	 * @param key
	 * @param ascendSort
	 */
	public KeyAndSort(String key, boolean ascendSort) {
		this.key = key;
		this.ascendSort = ascendSort;
	}

	/**
	 * カラムキーを求める(<b>Korean</b> : 칼럼 키를 구한다.)
	 * 
	 * @return
	 */
	public String getKey() {
		return key;
	}

	/**
	 * 昇順にソートされているかどうかを確認する。(<b>Korean</b> : 오름차순 정렬인지 여부를 확인한다.)
	 * 
	 * @return
	 */
	public boolean isAscendSort() {
		return ascendSort;
	}

	public String toString() {
		return key;
	}
}
