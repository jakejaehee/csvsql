package jake.csvsql;

/**
 * グルーピングに使用されるキー(<b>Korean</b> : 그룹핑에 사용될 키)
 * 
 * @author Jake Lee
 */
public class GroupKeys extends AbstractQueryKeys {

	public GroupKeys() {
	}

	/**
	 * グルーピングに使用されるキーを設定する。(<b>Korean</b> : 그룹핑에 사용할 키를 설정한다.)
	 * 
	 * @param key
	 */
	public GroupKeys(String key) {
		super(key);
	}

	/**
	 * グルーピングに使用されるキーを設定し、レコードはグループ内でソートされる。(<b>Korean</b> : 그룹핑에 사용할 키를 설정하며 그룹
	 * 내에서 레코드들은 정렬이 된다.)
	 * 
	 * @param key
	 *            (<b>Korean</b> : 그룹핑 키)
	 * @param ascendSort
	 *            (<b>Korean</b> : true이면 오름차순 정렬, false이면 내림차순 정렬)
	 */
	public GroupKeys(String key, boolean ascendSort) {
		super(key, ascendSort);
	}

	/**
	 * グルーピングに使用するキーを設定する。(<b>Korean</b> : 그룹핑에 사용할 키를 설정한다.)
	 */
	public GroupKeys and(String key) {
		return and(key, true);
	}

	/**
	 * グルーピングに使用するキーを設定し、グループ内のレコードはソートされる。(<b>Korean</b> : 그룹핑에 사용할 키를 설정하며 그룹
	 * 내에서 레코드들은 정렬이 된다.)
	 * 
	 * @param key
	 *            (<b>Korean</b> : 그룹핑 키)
	 * @param ascendSort
	 *            (<b>Korean</b> : true이면 오름차순 정렬, false이면 내림차순 정렬)
	 */
	public GroupKeys and(String key, boolean ascendSort) {
		add(new KeyAndSort(key, ascendSort));
		return this;
	}
}
