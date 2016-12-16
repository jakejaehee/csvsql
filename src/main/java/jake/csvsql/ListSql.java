package jake.csvsql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import elastic.util.dataset.DataSet;
import elastic.util.util.StringUtil;
import elastic.web.dataset.WebDataSet;

/**
 * List<Map>を処理する格種methodの集まりである。SQL文を使い、DBでデータを処理することと同じ方式で実現されている。
 * 但し、SQLのようにscriptを用いるのではなく、methodを呼び出す方式である。(<b>Korean</b> : List<Map>를 처리하는
 * 각종 method들의 모음이다. DB에서 SQL문을 이용하여 데이터를 처리하는 것과 같은 방식으로 구현되어 있다. 단 SQL과 같은
 * script를 사용하는 것이 아니라 method를 호출하는 방식이다.)
 * 
 * @author Jake Lee
 */
public class ListSql {
	private static final Logger LOG = Logger.getLogger(ListSql.class);

	private static int indexOf(final List<Map> dstList,
			final Conditions conditions) {
		if (dstList == null || dstList.size() == 0)
			return -1;
		if (conditions == null)
			return 0;

		int size = dstList.size();
		for (int i = 0; i < size; i++) {
			Map record = dstList.get(i);
			if (conditions.suits(record))
				return i;
		}
		return -1;
	}

	private static List<List<Map>> _groupSortedList(final List<Map> sortedList,
			Conditions conditions, final GroupKeys groupKeys) {
		if (groupKeys == null || groupKeys.size() == 0) {
			throw new RuntimeException(
					"GroupKeys is required but no GroupKeys.");
		}

		List<List<Map>> groupList = new ArrayList<List<Map>>();
		List<Map> group = new ArrayList<Map>();

		Object[] prevVals = new String[groupKeys.size()];

		int size = sortedList.size();
		for (int i = 0; i < size; i++) {
			Map record = sortedList.get(i);
			if (conditions != null && !conditions.suits(record))
				continue;

			Object[] vals = new String[groupKeys.size()];
			for (int k = 0; k < groupKeys.size(); k++)
				vals[k] = record.get(groupKeys.get(k).getKey());

			int com = ListSqlUtil.compare(prevVals, vals);
			if (com == 0) {
				group.add(record);
			} else {
				if (group.size() > 0) {
					groupList.add(group);
					group = new ArrayList<Map>();
				}
				group.add(record);
				System.arraycopy(vals, 0, prevVals, 0, vals.length);
			}
		}
		if (group.size() > 0)
			groupList.add(group);

		return groupList;
	}

	/**
	 * ソートされたListに重複のレコードが存在しないように、sKeysをキーにしてListを作成し返す。(<b>Korean</b> : 정렬된
	 * List에서 sKeys를 key로 하여 레코드가 중복되지 않도록 List를 만들어서 반환한다.)
	 * 
	 * @param sortedList
	 *            sKeysでソートされたList&lt;Map&gt;(<b>Korean</b> : sKeys로 정렬된
	 *            List&lt;Map&gt;)
	 * @param distinctKeys
	 *            基準となるカラムキー(<b>Korean</b> : 기준이 되는 칼럼 키)
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @return
	 */
	public static List<Map> distinctSortedList(final List<Map> sortedList,
			final SelectKeys distinctKeys, final Conditions conditions,
			final boolean selectAllColumns) {
		List<Map> ret = new ArrayList<Map>();

		List<Map> _sortedList = select(sortedList, conditions);

		Object[] prevVals = new String[distinctKeys.size()];

		int size = _sortedList.size();
		for (int i = 0; i < size; i++) {
			Map row = _sortedList.get(i);
			Object[] vals = new String[distinctKeys.size()];
			for (int k = 0; k < distinctKeys.size(); k++) {
				vals[k] = row.get(distinctKeys.get(k).getKey());
			}

			int com = ListSqlUtil.compare(prevVals, vals);
			if (com != 0) {
				Map cloned = null;
				if (selectAllColumns)
					cloned = ListSqlUtil.cloneRecord(row);
				else
					cloned = ListSqlUtil.cloneRecord(row,
							distinctKeys.getKeys());
				ret.add(cloned);
				System.arraycopy(vals, 0, prevVals, 0, vals.length);
			}
		}

		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("distinctSortedList ");
			sb.append(distinctKeys);
			sb.append(" from ");
			if (sortedList instanceof DataSet)
				sb.append(((DataSet) sortedList).getName());
			else
				sb.append("sortedList");
			if (conditions != null)
				sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret;
	}

	/**
	 * ソートされたListに重複のレコードが存在しないように、sKeysをキーにしてListを作成し返す。(<b>Korean</b> : 정렬된
	 * List에서 sKeys를 key로 하여 레코드가 중복되지 않도록 List를 만들어서 반환한다.)
	 * 
	 * @param sortedList
	 *            sKeysでソートされたList&lt;Map&gt;(<b>Korean</b> : sKeys로 정렬된
	 *            List&lt;Map&gt;)
	 * @param distinctKeys
	 *            基準となるカラムキー(<b>Korean</b> : 기준이 되는 칼럼 키)
	 * @return
	 */
	public static List<Map> distinctSortedList(List<Map> sortedList,
			SelectKeys distinctKeys, boolean selectAllColumns) {
		return distinctSortedList(sortedList, distinctKeys, null,
				selectAllColumns);
	}

	/**
	 * 与えられたListに重複したレコードが存在しないように、sKeysをキーにしてリスト作成し返す。(<b>Korean</b> : 주어진
	 * List에서 sKeys를 key로 하여 레코드가 중복되지 않도록 List를 만들어서 반환한다.)
	 * 
	 * @param list
	 *            原本List&lt;Map&gt; (<b>Korean</b> : 원본 List&lt;Map&gt;)
	 * @param distinctKeys
	 *            基準となるカラムキー (<b>Korean</b> : 기준이 되는 칼럼 키)
	 * @param conditions
	 *            SQL文のwhere句に該当する条件値 (<b>Korean</b> : SQL문의 where 구문에 해당하는 조건
	 *            값)
	 * @return
	 */
	public static List<Map> distinct(List<Map> list, SelectKeys distinctKeys,
			Conditions conditions, boolean selectAllColumns) {
		if (distinctKeys == null || distinctKeys.size() == 0) {
			throw new RuntimeException(
					"In distinct() method, SelectKeys is required.");
		}
		SortKeys sortKeys = distinctKeys.toSortKeys();
		sort(list, sortKeys);
		return distinctSortedList(list, distinctKeys, conditions,
				selectAllColumns);
	}

	/**
	 * 与えられたListに重複したレコードが存在しないように、sKeysをキーにしてリスト作成し返す。(<b>Korean</b> : 주어진
	 * List에서 sKeys를 key로 하여 레코드가 중복되지 않도록 List를 만들어서 반환한다.)
	 * 
	 * @param list
	 *            原本List&lt;Map&gt; (<b>Korean</b> : 원본 List&lt;Map&gt;)
	 * @param sKeys
	 *            基準となるカラムキー (<b>Korean</b> : 기준이 되는 칼럼 키)
	 * @return
	 */
	public static List<Map> distinct(List<Map> list, SelectKeys sKeys,
			boolean selectAllColumns) {
		return distinct(list, sKeys, null, selectAllColumns);
	}

	private static List<List<Map>> _groupAgain(List<List<Map>> groupLists,
			final String key, final boolean ascendSort) {
		List<List<Map>> reGroup = new ArrayList<List<Map>>();
		int size = groupLists.size();
		for (int r = 0; r < size; r++) {
			List<List<Map>> subGrouped = _group(groupLists.get(r), key,
					ascendSort, null);
			int size2 = subGrouped.size();
			for (int s = 0; s < size2; s++) {
				List<Map> group = subGrouped.get(s);
				if (group != null && group.size() > 0)
					reGroup.add(group);
			}
		}
		return reGroup;
	}

	private static List<List<Map>> _group(List<Map> list, final String gKey,
			final boolean ascendSort, Conditions conditions) {
		_sort(list, gKey, ascendSort);
		List<List<Map>> groupList = _groupSortedList(list, conditions,
				new GroupKeys(gKey));
		return groupList;
	}

	/**
	 * Listのレコードをグルーピングする。(<b>Korean</b> : List의 레코드들을 Grouping 한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param gKeys
	 *            Grouping Key
	 * @return グループリスト(List of groups) (<b>Korean</b> : 그룹들의 목록(List of groups))
	 */
	public static List<List<Map>> group(List<Map> list, GroupKeys gKeys) {
		return group(list, null, gKeys);
	}

	/**
	 * Listから条件に該当するレコードのみグルーピングする。 (<b>Korean</b> : List에서 조건에 해당하는 레코드들만
	 * Grouping 한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件 <b>(Korean :</b> 검색 조건<b>)</b>
	 * @param gKeys
	 *            Grouping Key
	 * @return グループリスト(List of groups) (<b>Korean</b> : 그룹들의 목록(List of groups))
	 */
	public static List<List<Map>> group(List<Map> list, Conditions conditions,
			GroupKeys gKeys) {
		if (gKeys == null || gKeys.size() == 0) {
			throw new RuntimeException(
					"GroupKeys is required but no GroupKeys.");
		}
		List<List<Map>> grouped = _group(list, gKeys.get(0).getKey(), gKeys
				.get(0).isAscendSort(), conditions);
		for (int k = 1; k < gKeys.size(); k++)
			grouped = _groupAgain(grouped, gKeys.get(k).getKey(), gKeys.get(k)
					.isAscendSort());
		return grouped;
	}

	/**
	 * 各Groupのレコードを、sortKeysを基準にソートする。(<b>Korean</b> : 각 Group들 안의 레코드들을
	 * sortKeys를 기준으로 정렬한다.)
	 * 
	 * @param groupLists
	 *            グループリスト(List&lt;List&lt;Map&gt;&gt;)(<b>Korean</b> : 그룹들의
	 *            목록(List&lt;List&lt;Map&gt;&gt;))
	 * @param sortKeys
	 *            ソートの基準となるカラムキー (<b>Korean</b> : 정렬 기준 칼럼 키)
	 */
	public static void sortEachGroup(List<List<Map>> groupLists,
			SortKeys sortKeys) {
		int size = groupLists.size();
		for (int g = 0; g < size; g++) {
			List<Map> group = groupLists.get(g);
			if (group != null && group.size() > 0) {
				sort(group, sortKeys);
			}
		}
	}

	/**
	 * sortKeysを基準に与えられたListを昇順にソートしたリストを返す。 (<b>Korean</b> : 주어진 List를
	 * sortKeys를 기준으로 오름차순으로 정렬한다.)
	 * 
	 * @param list
	 *            原本List&lt;Map&gt; (<b>Korean</b> : 원본 List&lt;Map&gt;)
	 * @param sortKeys
	 *            ソートの基準となるカラムキー (<b>Korean</b> : 정렬의 기준이 되는 칼럼 키)
	 */
	public static void sort(List<Map> list, SortKeys sortKeys) {
		if (sortKeys.size() == 1) {
			_sort(list, sortKeys.get(0).getKey(), sortKeys.get(0)
					.isAscendSort());
		} else {
			List<Map> sorted = new ArrayList<Map>();
			List<List<Map>> grouped = _group(list, sortKeys.get(0).getKey(),
					sortKeys.get(0).isAscendSort(), null);
			int size = sortKeys.size();
			for (int k = 1; k < size; k++) {
				if (k < sortKeys.size() - 1) {
					grouped = _groupAgain(grouped, sortKeys.get(k).getKey(),
							sortKeys.get(k).isAscendSort());
				} else {
					int max = grouped.size();
					for (int g = 0; g < max; g++) {
						List<Map> group = grouped.get(g);
						_sort(group, sortKeys.get(k).getKey(), sortKeys.get(k)
								.isAscendSort());
						sorted.addAll(group);
					}
					break;
				}
			}
			list.clear();
			list.addAll(sorted);
		}
	}

	/**
	 * sortKeyを基準に、与えられたListをソートしたListを返す。(<b>Korean</b> : 주어진 List를 sortKey를
	 * 기준으로 정렬한다.)
	 * 
	 * @param list
	 *            原本 List&lt;Map&gt; (<b>Korean</b> : 원본 List&lt;Map&gt;)
	 * @param sortKey
	 *            ソートの基準となるカラムキー (<b>Korean</b> : 정렬의 기준이 되는 칼럼 키)
	 * @param ascendSort
	 *            ソート方式(true: 昇順, false: 降順) (<b>Korean</b> : 정렬 방법(true: 오름차순,
	 *            false: 내림차순))
	 */
	private static void _sort(final List<Map> list, final String sortKey,
			final boolean ascendSort) {
		if (!StringUtil.isEmpty(sortKey)) {
			Collections.sort(list, new Comparator<Map>() {
				public int compare(Map d1, Map d2) {
					int com = ListSqlUtil.compare(d1.get(sortKey),
							d2.get(sortKey));
					return ascendSort ? com : -com;
				}
			});
		}
	}

	/**
	 * 与えられたListからminKeyカラムの最小値を求める。(<b>Korean</b> : 주어진 List에서 minKey 칼럼의 최소값을
	 * 구한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param minKey
	 *            対象となるカラムキー (<b>Korean</b> : 대상 칼럼 키)
	 * @return
	 */
	public static Object getMinValue(final List<Map> list, final String minKey) {
		Map row = selectMinRecord(list, minKey);
		return row != null ? row.get(minKey) : null;
	}

	/**
	 * 与えられたListからminKeyカラムの最大値を求める。(<b>Korean</b> : 주어진 List에서 minKey 칼럼의 최대값을
	 * 구한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param maxKey
	 *            対象となるカラムキー (<b>Korean</b> : 대상 칼럼 키)
	 * @return
	 */
	public static Object getMaxValue(final List<Map> list, final String maxKey) {
		Map row = selectMaxRecord(list, maxKey);
		return row != null ? row.get(maxKey) : null;
	}

	/**
	 * 与えられたリストをgroupKeysを基準にグルーピングし、当グループの中からminKeyカラムが最大値 を持つレコードのリストを作成し、返す。
	 * 但し、groupKeysとminKeyに該当するカラムのみがレコードに構成される。(<b>Korean</b> : 주어진 List를
	 * groupKeys를 기준으로 그룹핑하고 그 그룹안에서 minKey 칼럼이 최소값인 레코드의 목록을 만들어서 반환한다. 단,
	 * groupKeys와 minKey에 해당하는 칼럼만 레코드로 구성된다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param groupKeys
	 *            グルーピングの基準となるキー (<b>Korean</b> : 그룹핑 기준 키)
	 * @param minKey
	 *            最小値を求めるキー (<b>Korean</b> : 최소값을 구할 대상 키)
	 * @return
	 */
	public static List<Map> selectMin(final List<Map> list,
			final GroupKeys groupKeys, final String minKey) {
		return selectMin(list, null, groupKeys, minKey, false);
	}

	/**
	 * 与えられたリストをgroupKeysを基準にグルーピングし、当グループの中からminKeyカラムが最大値 を持つレコードのリストを作成し、返す。
	 * 但し、groupKeysとminKeyに該当するカラムのみがレコードに構成される。(<b>Korean</b> : 주어진 List를
	 * groupKeys를 기준으로 그룹핑하고 그 그룹안에서 minKey 칼럼이 최소값인 레코드의 목록을 만들어서 반환한다. 단,
	 * groupKeys와 minKey에 해당하는 칼럼만 레코드로 구성된다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @param groupKeys
	 *            グルーピングの基準となるキー (<b>Korean</b> : 그룹핑 기준 키)
	 * @param minKey
	 *            最小値を求めるキー (<b>Korean</b> : 최소값을 구할 대상 키)
	 * @return
	 */
	public static List<Map> selectMin(final List<Map> list,
			Conditions conditions, final GroupKeys groupKeys,
			final String minKey) {
		return selectMin(list, conditions, groupKeys, minKey, false);
	}

	/**
	 * 与えられたminKeyのカラムの値が最小のレコードコピー本を返す。<b>(Korean :</b> 주어진 minKey의 칼럼 값이 최소값인
	 * 레코드의 복사본을 반환한다.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param minKey
	 *            最小値を求めるキー (<b>Korean</b> : 최소값을 구할 대상 키)
	 * @return
	 */
	public static Map selectMinRecord(final List<Map> list, final String minKey) {
		return selectMinRecord(list, null, minKey);
	}

	/**
	 * 与えられたminKeyのカラムの値が最小のレコードコピー本を返す。但し、conditoinsの条件に該当するレコードのみ対象に検索する。
	 * <b>(Korean :</b> 주어진 minKey의 칼럼 값이 최소값인 레코드의 복사본을 반환한다. 단, conditoins의
	 * 조건에 해당하는 레코드들만 대상으로 검색한다.<b>)</b>
	 * 
	 * @param list
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @param minKey
	 * @return
	 */
	public static Map selectMinRecord(final List<Map> list,
			Conditions conditions, final String minKey) {
		Map minRecord = findMinRecord(list, conditions, minKey);
		return ListSqlUtil.cloneRecord(minRecord);
	}

	/**
	 * 与えられたminKeyのカラムの値が最小値のレコード原本を返却する。 <b>(Korean :</b> 주어진 minKey의 칼럼 값이
	 * 최소값인 레코드의 원본을 반환한다.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param minKey
	 *            最小値を求めるキー (<b>Korean</b> : 최소값을 구할 대상 키)
	 * @return
	 */
	public static Map findMinRecord(final List<Map> list, final String minKey) {
		return findMinRecord(list, null, minKey);
	}

	/**
	 * 与えられたminKeyのカラムの値が最小値のレコード原本を返却する。但し、conditoinsの条件に該当するレコードのみ対象に検索する。
	 * <b>(Korean :</b> 주어진 minKey의 칼럼 값이 최소값인 레코드의 원본을 반환한다. 단, conditoins의 조건에
	 * 해당하는 레코드들만 대상으로 검색한다.<b>)</b>
	 * 
	 * @param list
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @param minKey
	 * @return
	 */
	public static Map findMinRecord(final List<Map> list,
			Conditions conditions, final String minKey) {
		List<Map> tmp = conditions != null ? find(list, conditions, false)
				: list;
		Map minRecord = null;
		Object min = null;
		int size = tmp.size();
		for (int r = 0; r < size; r++) {
			Map record = tmp.get(r);
			Object val = record.get(minKey);
			if (r == 0 || ListSqlUtil.compare(min, val) > 0) {
				min = val;
				minRecord = record;
			}
		}
		return minRecord;
	}

	/**
	 * groupKeysを基準に与えられたListをグルーピングし、当グループの中からminKeyカラム が最小値を持つレコードのリストを作成し、返す。
	 * すべてのカラムが取得できる。(<b>Korean</b> : 주어진 List를 groupKeys를 기준으로 그룹핑하고 그 그룹안에서
	 * minKey 칼럼이 최소값인 레코드의 목록을 만들어서 반환한다. 모든 칼럼이 구해진다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param groupKeys
	 *            グルーピングの基準となるキー (<b>Korean</b> : 그룹핑 기준 키)
	 * @param minKey
	 *            最小値を求めるキー (<b>Korean</b> : 최소값을 구할 대상 키)
	 * @return
	 */
	public static List<Map> selectMinWithOtherKeys(final List<Map> list,
			final GroupKeys groupKeys, final String minKey) {
		return selectMin(list, null, groupKeys, minKey, true);
	}

	/**
	 * groupKeysを基準に与えられたListをグルーピングし、当グループの中からminKeyカラム が最小値を持つレコードのリストを作成し、返す。
	 * すべてのカラムが取得できる。(<b>Korean</b> : 주어진 List를 groupKeys를 기준으로 그룹핑하고 그 그룹안에서
	 * minKey 칼럼이 최소값인 레코드의 목록을 만들어서 반환한다. 모든 칼럼이 구해진다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @param groupKeys
	 *            グルーピングの基準となるキー (<b>Korean</b> : 그룹핑 기준 키)
	 * @param minKey
	 *            最小値を求めるキー (<b>Korean</b> : 최소값을 구할 대상 키)
	 * @return
	 */
	public static List<Map> selectMinWithOtherKeys(final List<Map> list,
			Conditions conditions, final GroupKeys groupKeys,
			final String minKey) {
		return selectMin(list, conditions, groupKeys, minKey, true);
	}

	/**
	 * 与えられたリストをgroupKeysを基準にグルーピングし、当グループの中からminKeyカラムが最大値 を持つレコードのリストを作成し、返す。
	 * 但し、groupKeysとminKeyに該当するカラムのみがレコードに構成される。(<b>Korean</b> : 주어진 List를
	 * groupKeys를 기준으로 그룹핑하고 그 그룹안에서 minKey 칼럼이 최소값인 레코드의 목록을 만들어서 반환한다. 단,
	 * groupKeys와 minKey에 해당하는 칼럼만 레코드로 구성된다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param groupKeys
	 *            グルーピングの基準となるキー (<b>Korean</b> : 그룹핑 기준 키)
	 * @param minKey
	 *            最小値を求めるキー (<b>Korean</b> : 최소값을 구할 대상 키)
	 * @return
	 */
	public static List<Map> findMin(final List<Map> list,
			final GroupKeys groupKeys, final String minKey) {
		return findMin(list, null, groupKeys, minKey);
	}

	/**
	 * groupKeysを基準に与えられたListをグルーピングし、 当グループの中からmaxKeyカラムが最大値を持つレコードのリストを作成し、返す。
	 * 但し、groupKeysとmaxKeyに該当するカラムのみがレコードに構成される。(<b>Korean</b> : 주어진 List를
	 * groupKeys를 기준으로 그룹핑하고 그 그룹안에서 maxKey 칼럼이 최대값인 레코드의 목록을 만들어서 반환한다. 단,
	 * groupKeys와 maxKey에 해당하는 칼럼만 레코드로 구성된다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param groupKeys
	 *            グルーピングの基準となるキー (<b>Korean</b> : 그룹핑 기준 키)
	 * @param maxKey
	 *            (<b>Korean</b> : 최소값을 구할 대상 키)
	 * @return
	 */
	public static List<Map> selectMax(final List<Map> list,
			final GroupKeys groupKeys, final String maxKey) {
		return selectMax(list, null, groupKeys, maxKey, false);
	}

	/**
	 * groupKeysを基準に与えられたListをグルーピングし、 当グループの中からmaxKeyカラムが最大値を持つレコードのリストを作成し、返す。
	 * 　但し、groupKeysとmaxKeyに該当するカラムのみがレコードに構成される。また、conditionsに該当するレコードのみを求める。(<
	 * b>Korean</b> : 주어진 List를 groupKeys를 기준으로 그룹핑하고 그 그룹안에서 maxKey 칼럼이 최대값인
	 * 레코드의 목록을 만들어서 반환한다. 단, groupKeys와 maxKey에 해당하는 칼럼만 레코드로 구성된다. 또한
	 * conditions에 해당하는 레코드들만 구한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            取得する条件値(SQL文のwhere句に該当する。) (<b>Korean</b> : 취득할 조건 값(SQL문의
	 *            where 구문에 해당))
	 * @param groupKeys
	 *            グルーピングキー (<b>Korean</b> : 그룹핑 키)
	 * @param maxKey
	 *            最大値を求めるキー (<b>Korean</b> : 최대값을 구할 칼럼 키)
	 * @return
	 */
	public static List<Map> selectMax(final List<Map> list,
			final Conditions conditions, final GroupKeys groupKeys,
			final String maxKey) {
		return selectMax(list, conditions, groupKeys, maxKey, false);
	}

	/**
	 * 与えられたmaxKeyのカラムの値が最大値のレコードコピー本を返却する。 <b>(Korean :</b> 주어진 maxKey의 칼럼 값이
	 * 최대값인 레코드의 복사본을 반환한다.<b>)</b>
	 * 
	 * @param list
	 * @param maxKey
	 * @return
	 */
	public static Map selectMaxRecord(final List<Map> list, final String maxKey) {
		return selectMaxRecord(list, null, maxKey);
	}

	/**
	 * 与えられたmaxKeyのカラムの値が最大値のレコードコピー本を返却する。但し、conditoinsの条件に該当するレコードのみ対象に検索する。
	 * <b>(Korean :</b> 주어진 maxKey의 칼럼 값이 최대값인 레코드의 복사본을 반환한다. 단, conditoins의
	 * 조건에 해당하는 레코드들만 대상으로 검색한다.<b>)</b>
	 * 
	 * @param list
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @param maxKey
	 * @return
	 */
	public static Map selectMaxRecord(final List<Map> list,
			Conditions conditions, final String maxKey) {
		Map maxRow = findMaxRecord(list, conditions, maxKey);
		return ListSqlUtil.cloneRecord(maxRow);
	}

	/**
	 * 与えられたmaxKeyのカラムの値が最大値のレコード原本を返却する。 <b>(Korean :</b> 주어진 maxKey의 칼럼 값이
	 * 최대값인 레코드의 원본을 반환한다.<b>)</b>
	 * 
	 * @param list
	 * @param maxKey
	 * @return
	 */
	public static Map findMaxRecord(final List<Map> list, final String maxKey) {
		return findMaxRecord(list, null, maxKey);
	}

	/**
	 * 与えられたmaxKeyのカラムの値が最大値のレコード原本を返却する。但し、conditoinsの条件に該当するレコードのみ対象に検索する。
	 * <b>(Korean :</b> 주어진 maxKey의 칼럼 값이 최대값인 레코드의 원본을 반환한다. 단, conditoins의 조건에
	 * 해당하는 레코드들만 대상으로 검색한다.<b>)</b>
	 * 
	 * @param list
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @param maxKey
	 * @return
	 */
	public static Map findMaxRecord(final List<Map> list,
			Conditions conditions, final String maxKey) {
		List<Map> tmp = conditions != null ? find(list, conditions, false)
				: list;
		Map maxRow = null;
		Object max = null;
		int size = tmp.size();
		for (int r = 0; r < size; r++) {
			Map row = tmp.get(r);
			Object val = row.get(maxKey);
			if (r == 0 || ListSqlUtil.compare(max, val) < 0) {
				max = val;
				maxRow = row;
			}
		}
		return maxRow;
	}

	/**
	 * groupKeysを基準に与えられたListをグルーピングし、当グループの中からmaxKeyカラムが最小値を持つレコードのリストを作成し、返す。
	 * すべてのカラムが取得できる。 (<b>Korean</b> : 주어진 List를 groupKeys를 기준으로 그룹핑하고 그 그룹안에서
	 * maxKey 칼럼이 최소값인 레코드의 목록을 만들어서 반환한다. 모든 칼럼이 구해진다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param groupKeys
	 *            グルーピングキー (<b>Korean</b> : 그룹핑 키)
	 * @param maxKey
	 *            最大値を求めるキー (<b>Korean</b> : 최대값을 구할 칼럼 키)
	 * @return
	 */
	public static List<Map> selectMaxWithOtherKeys(final List<Map> list,
			final GroupKeys groupKeys, final String maxKey) {
		return selectMax(list, null, groupKeys, maxKey, true);
	}

	/**
	 * groupKeysを基準に与えられたListをグルーピングし、当グループの中からmaxKeyカラムが最小値を持つレコードのリストを作成し、返す。
	 * すべてのカラムが取得できる。 (<b>Korean</b> : 주어진 List를 groupKeys를 기준으로 그룹핑하고 그 그룹안에서
	 * maxKey 칼럼이 최소값인 레코드의 목록을 만들어서 반환한다. 모든 칼럼이 구해진다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @param groupKeys
	 *            グルーピングキー (<b>Korean</b> : 그룹핑 키)
	 * @param maxKey
	 *            最大値を求めるキー (<b>Korean</b> : 최대값을 구할 칼럼 키)
	 * @return
	 */
	public static List<Map> selectMaxWithOtherKeys(final List<Map> list,
			Conditions conditions, final GroupKeys groupKeys,
			final String maxKey) {
		return selectMax(list, conditions, groupKeys, maxKey, true);
	}

	/**
	 * groupKeysを基準に与えられたListをグルーピングし、sumKeyカラムの値を集計したリストを作成し、返す。 (<b>Korean</b>
	 * : 주어진 List를 groupKeys로 그룹핑한 후 sumKey 칼럼의 값을 합한 List를 만들어서 반환한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param groupKeys
	 *            グルーピングキー (<b>Korean</b> : 그룹핑 키)
	 * @param sumKey
	 *            集計さするカラム(4bytesサイズの数字) (<b>Korean</b> : 합산할 칼럼(4bytes 크기 숫자))
	 * @return
	 */
	public static List<Map> selectSumInt(final List<Map> list,
			final GroupKeys groupKeys, final String sumKey) {
		return selectSum(list, groupKeys, sumKey, 0, true);
	}

	/**
	 * groupKeysを基準に与えられたListをグルーピングし、sumKeyカラムの値を集計したリストを作成し、返す。 (<b>Korean</b>
	 * : 주어진 List를 groupKeys로 그룹핑한 후 sumKey 칼럼의 값을 합한 List를 만들어서 반환한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param groupKeys
	 *            グルーピングキー (<b>Korean</b> : 그룹핑 키)
	 * @param sumKey
	 *            集計さするカラム(8bytesサイズの数字) (<b>Korean</b> : 합산할 칼럼(8bytes 크기 숫자))
	 * @return
	 */
	public static List<Map> selectSumDouble(final List<Map> list,
			final GroupKeys groupKeys, final String sumKey) {
		return selectSum(list, groupKeys, sumKey, 1, true);
	}

	private static List<Map> selectSum(final List<Map> list,
			final GroupKeys groupKeys, final String sumKey, int numType,
			boolean withOtherKeys) {
		List<Map> ret = new ArrayList<Map>();

		String[] selectKeys = null;
		if (withOtherKeys) {
			selectKeys = new String[groupKeys.size() + 1];
			System.arraycopy(groupKeys.getKeys(), 0, selectKeys, 0,
					groupKeys.size());
			selectKeys[selectKeys.length - 1] = sumKey;
		}

		if (list == null || list.size() == 0) {

		// 레코드가 하나 일때 해당 값이 Integer로 넣어지지 않아서 수정함.강석진(2015.11.10).이재희소장님 컨펌한 내용.
//		} else if (list.size() == 1) {
//			if (withOtherKeys)
//				ret.add(list.get(0));
//			else
//				ret.add(ListSqlUtil.cloneRecord(list.get(0), selectKeys));
		} else {
			List<List<Map>> groups = group(list, groupKeys);
			int size = groups.size();
			for (int g = 0; g < size; g++) {
				List<Map> group = groups.get(g);
				Map row = group.get(0);
				if (withOtherKeys)
					row = ListSqlUtil.cloneRecord(row);
				else
					row = ListSqlUtil.cloneRecord(row, selectKeys);
				if (numType == 0)
					row.put(sumKey, getSumValueInInt(group, sumKey));
				else
					row.put(sumKey, getSumValueInDouble(group, sumKey));
				ret.add(row);
			}
		}
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("selectSum ").append(sumKey);
			if (withOtherKeys)
				sb.append(", *");
			sb.append(" from ");
			if (list instanceof DataSet)
				sb.append(((DataSet) list).getName());
			else
				sb.append("List");
			if (groupKeys != null)
				sb.append(" group by ").append(groupKeys);
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret;
	}

	private static List<Map> selectMin(final List<Map> list,
			Conditions conditions, final GroupKeys groupKeys,
			final String minKey, boolean withOtherKeys) {
		List<Map> ret = new ArrayList<Map>();

		String[] selectKeys = null;
		if (withOtherKeys) {
			selectKeys = new String[groupKeys.size() + 1];
			System.arraycopy(groupKeys.getKeys(), 0, selectKeys, 0,
					groupKeys.size());
			selectKeys[selectKeys.length - 1] = minKey;
		}

		if (list == null || list.size() == 0) {

		} else if (list.size() == 1) {
			Map record = list.get(0);
			if (conditions == null || conditions.suits(record)) {
				if (withOtherKeys)
					ret.add(ListSqlUtil.cloneRecord(record));
				else
					ret.add(ListSqlUtil.cloneRecord(record, selectKeys));
			}
		} else {
			List<List<Map>> groups = group(list, conditions, groupKeys);
			int size = groups.size();
			for (int g = 0; g < size; g++) {
				List<Map> group = groups.get(g);
				Map row = selectMinRecord(group, minKey);
				if (withOtherKeys)
					ret.add(row);
				else
					ret.add(ListSqlUtil.cloneRecord(row, selectKeys));
			}
		}
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("selectMin ").append(minKey);
			if (withOtherKeys)
				sb.append(", *");
			sb.append(" from ");
			if (list instanceof DataSet)
				sb.append(((DataSet) list).getName());
			else
				sb.append("List");
			if (groupKeys != null)
				sb.append(" group by ").append(groupKeys);
			if (conditions != null)
				sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret;
	}

	/**
	 * 与えられたリストをgroupKeysを基準にグルーピングし、当グループの中からminKeyカラムが最大値 を持つレコードのリストを作成し、返す。
	 * 但し、groupKeysとminKeyに該当するカラムのみがレコードに構成される。(<b>Korean</b> : 주어진 List를
	 * groupKeys를 기준으로 그룹핑하고 그 그룹안에서 minKey 칼럼이 최소값인 레코드의 목록을 만들어서 반환한다. 단,
	 * groupKeys와 minKey에 해당하는 칼럼만 레코드로 구성된다.)
	 * <p>
	 * 
	 * <b>Note :</b><br>
	 * 原本を返却する。 <b>(Korean :</b> 원본을 반환한다.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @param groupKeys
	 *            グルーピングの基準となるキー (<b>Korean</b> : 그룹핑 기준 키)
	 * @param minKey
	 *            最小値を求めるキー (<b>Korean</b> : 최소값을 구할 대상 키)
	 * @return
	 */
	public static List<Map> findMin(final List<Map> list,
			Conditions conditions, final GroupKeys groupKeys,
			final String minKey) {
		List<Map> ret = new ArrayList<Map>();

		if (list == null || list.size() == 0) {

		} else if (list.size() == 1) {
			Map record = list.get(0);
			if (conditions == null || conditions.suits(record)) {
				ret.add(record);
			}
		} else {
			List<List<Map>> groups = group(list, conditions, groupKeys);
			int size = groups.size();
			for (int g = 0; g < size; g++) {
				List<Map> group = groups.get(g);
				Map row = findMinRecord(group, minKey);
				ret.add(row);
			}
		}
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("selectMin ").append(minKey);
			sb.append(", *");
			sb.append(" from ");
			if (list instanceof DataSet)
				sb.append(((DataSet) list).getName());
			else
				sb.append("List");
			if (groupKeys != null)
				sb.append(" group by ").append(groupKeys);
			if (conditions != null)
				sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret;
	}

	private static List<Map> selectMax(final List<Map> list,
			Conditions conditions, final GroupKeys groupKeys,
			final String maxKey, boolean withOtherKeys) {
		List<Map> ret = new ArrayList<Map>();

		String[] selectKeys = null;
		if (withOtherKeys) {
			selectKeys = new String[groupKeys.size() + 1];
			System.arraycopy(groupKeys.getKeys(), 0, selectKeys, 0,
					groupKeys.size());
			selectKeys[selectKeys.length - 1] = maxKey;
		}

		if (list == null || list.size() == 0) {

		} else if (list.size() == 1) {
			Map record = list.get(0);
			if (conditions == null || conditions.suits(record)) {
				if (withOtherKeys)
					ret.add(ListSqlUtil.cloneRecord(record));
				else
					ret.add(ListSqlUtil.cloneRecord(record, selectKeys));
			}
		} else {
			List<List<Map>> groups = group(list, conditions, groupKeys);
			int size = groups.size();
			for (int g = 0; g < size; g++) {
				List<Map> group = groups.get(g);
				Map maxRow = selectMaxRecord(group, maxKey);
				if (withOtherKeys)
					ret.add(maxRow);
				else
					ret.add(ListSqlUtil.cloneRecord(maxRow, selectKeys));
			}
		}
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("selectMax ").append(maxKey);
			if (withOtherKeys)
				sb.append(", *");
			sb.append(" from ");
			if (list instanceof DataSet)
				sb.append(((DataSet) list).getName());
			else
				sb.append("List");
			if (groupKeys != null)
				sb.append(" group by ").append(groupKeys);
			if (conditions != null)
				sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret;
	}

	/**
	 * List&lt;Map&gt;をgroupKeysを基準にグルーピングした後、各グループ内でmaxKeyカラムの値が最大値のレコードを削除する。
	 * <b>(Korean :</b> List&lt;Map&gt;을 groupKeys를 기준으로 그룹핑한 다음 각 그룹내에서 maxKey
	 * 칼럼의 값이 최대값인 레코드를 삭제한다.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param groupKeys
	 * @param maxKey
	 *            最大値を求めるキー (<b>Korean</b> : 최대값을 구할 칼럼 키)
	 */
	public static void deleteMax(final List<Map> list,
			final GroupKeys groupKeys, final String maxKey) {
		deleteMax(list, null, groupKeys, maxKey);
	}

	/**
	 * List&lt;Map&gt;からconditionsの条件に該当するレコードをgroupKeysを基準にグルーピングした後、
	 * 各グループ内でmaxKeyカラムの値が最大値のレコードを削除する。 <b>(Korean :</b> List&lt;Map&gt;에서
	 * conditions의 조건에 해당하는 레코드들을 groupKeys를 기준으로 그룹핑한 다음 각 그룹내에서 maxKey 칼럼의 값이
	 * 최대값인 레코드를 삭제한다.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            SQLのwhere句に該当する条件値(<b>Korean</b> : SQL의 where 구문에 해당하는 조건 값)
	 * @param groupKeys
	 * @param maxKey
	 *            最大値を求めるキー (<b>Korean</b> : 최대값을 구할 칼럼 키)
	 */
	public static void deleteMax(final List<Map> list,
			final Conditions conditions, final GroupKeys groupKeys,
			final String maxKey) {
		List<Map> ret = new ArrayList<Map>();

		if (list == null || list.size() == 0) {
			// } else if (list.size() == 1) {
		} else {
			List<List<Map>> groups = group(list, conditions, groupKeys);
			int size = groups.size();
			for (int g = 0; g < size; g++) {
				List<Map> group = groups.get(g);
				Map maxRecord = deleteMaxRecord(group, maxKey);
				if (maxRecord != null) {
					list.remove(maxRecord);
				}
			}
		}
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("deleteMax ").append(maxKey);
			sb.append(" from ");
			if (list instanceof DataSet)
				sb.append(((DataSet) list).getName());
			else
				sb.append("List");
			if (groupKeys != null)
				sb.append(" group by ").append(groupKeys);
			if (conditions != null)
				sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
	}

	private static final String KEY_OF_INDEX_OF_RECORD = "__index__";

	private static List<Map> selectWithIndex(final List<Map> list,
			final Conditions conditions, final SelectKeys selKeys) {
		final List<Map> tmp = new ArrayList<Map>();
		final String[] keyArr = selKeys != null ? selKeys.getKeys() : null;

		handleRecords(list, new RecordHandler() {
			public void handle(int index, Map record) {
				if (conditions == null || conditions.suits(record)) {
					Map newRecord = new HashMap();
					newRecord.put(KEY_OF_INDEX_OF_RECORD, index);
					if (keyArr != null) {
						for (int k = 0; k < keyArr.length; k++) {
							newRecord.put(keyArr[k], record.get(keyArr[k]));
						}
					} else {
						newRecord.putAll(record);
					}
					tmp.add(newRecord);
				}
			}
		});
		return tmp;
	}

	private static Map deleteMaxRecord(final List<Map> list, final String maxKey) {
		return deleteMaxRecord(list, null, maxKey);
	}

	private static Map deleteMaxRecord(final List<Map> list,
			final Conditions conditions, final String maxKey) {
		final List<Map> tmp = selectWithIndex(list, conditions, new SelectKeys(
				maxKey));

		Map removedRecord = null;
		Map maxRecord = null;
		Object max = null;
		int size = tmp != null ? tmp.size() : 0;
		for (int r = 0; r < size; r++) {
			Map record = tmp.get(r);
			Object val = record.get(maxKey);
			if (r == 0 || ListSqlUtil.compare(max, val) < 0) {
				max = val;
				maxRecord = record;
			}
		}
		if (maxRecord != null) {
			int index = (Integer) maxRecord.get(KEY_OF_INDEX_OF_RECORD);
			removedRecord = list.remove(index);
			if (LOG.isTraceEnabled()) {
				LOG.trace("delete " + removedRecord);
			}
		}
		return removedRecord;
	}

	/**
	 * 与えられたListからsumKeyカラムの値をすべて集計した数値(double型)を求める。(<b>Korean</b> : 주어진 List에서
	 * sumKey 칼럼의 값을 모두 합한 값(double 형)을 구한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param sumKey
	 *            集計するカラムキー (<b>Korean</b> : 합산할 칼럼 키)
	 * @return
	 */
	public static double getSumValueInDouble(final List<Map> list,
			final String sumKey) {
		double sum = 0;
		int size = list != null ? list.size() : 0;
		for (int i = 0; i < size; i++) {
			Map row = list.get(i);
			if (row != null)
				sum += DataUtil.toDouble(row.get(sumKey));
		}
		return sum;
	}

	/**
	 * 与えられたListからsumKeyカラムの値をすべて集計した数値(int型)を求める。 (<b>Korean</b> : 주어진 List에서
	 * sumKey 칼럼의 값을 모두 합한 값(int 형)을 구한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param sumKey
	 *            集計するカラムキー (<b>Korean</b> : 합산할 칼럼 키)
	 * @return
	 */
	public static int getSumValueInInt(final List<Map> list, final String sumKey) {
		int sum = 0;
		int size = list != null ? list.size() : 0;
		for (int i = 0; i < size; i++) {
			Map row = list.get(i);
			if (row != null) {
				sum += DataUtil.toInt(row.get(sumKey));
			}
		}
		return sum;
	}

	/**
	 * 与えられたListから条件(conditions)に合うレコードを探し、当レコードの前に新規レコード(newRecord)を挿入する。
	 * appendIfNotFoundがtrueであるかつ、条件に合うレコードがない場合には最下段に新規レコード(newRecord)を追加する。
	 * レコードを追加した場合には、trueを返し、その以外の場合にはfalseを返す。 (<b>Korean</b> : 주어진 List에서
	 * 조건(conditions)에 맞는 레코드를 찾아서 찾은 레코드의 앞에 새로운 레코드(newRecord)를 삽입한다.
	 * appendIfNotFound가 true일 경우에 조건에 맞는 레코드가 없을 경우에는 맨 마지막에 새로운
	 * 레코드(newRecord)를 추가한다. 레코드를 추가했을 경우에는 true를 반환하며 그외의 경우에는 false를 반환한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            条件値(SQL文のwhere句に該当) (<b>Korean</b> : 조건 값(SQL문의 where 구문에 해당))
	 * @param newRecord
	 *            新規レコード (<b>Korean</b> : 새로운 레코드)
	 * @param appendIfNotFound
	 * @return
	 */
	public static boolean insertAt(final List<Map> list,
			final Conditions conditions, final Map newRecord,
			final boolean appendIfNotFound) {
		if (list == null)
			return false;
		int idx = indexOf(list, conditions);
		if (idx != -1) {
			list.add(idx, newRecord);
			return true;
		} else {
			if (appendIfNotFound) {
				list.add(newRecord);
				return true;
			}
		}
		return false;
	}

	/**
	 * 与えられたListから条件(conditions)に合う一番目のレコードをコピーして返す。
	 * 返されたレコードを修正しても原本のレコードには反映されない。 (<b>Korean</b> : 주어진 List에서
	 * 조건(conditions)에 맞는 첫번째 레코드를 복사하여 반환한다. 반환된 레코드를 수정해도 원본 레코드는 수정되지 않는다.)
	 * 
	 * @param list
	 * @param conditions
	 * @return
	 */
	public static Map selectFirst(final List<Map> list,
			final Conditions conditions) {
		return _selectFirst(list, conditions, true, true);
	}

	/**
	 * 与えられたListから条件(conditions)に合う一番目のレコードを返す。返されたレコードを修正すると、原本にも反映される。
	 * (<b>Korean</b> : 주어진 List에서 조건(conditions)에 맞는 첫번째 레코드를 반환한다. 반환된 레코드를
	 * 수정하면 원본 레코드가 수정된다.)
	 * 
	 * @param list
	 * @param conditions
	 * @return
	 */
	public static Map findFirst(final List<Map> list,
			final Conditions conditions) {
		return _selectFirst(list, conditions, true, false);
	}

	/**
	 * 与えられたListから条件(conditions)に合う一番目のレコードを返す。返されたレコードを修正すると、原本も修正される。
	 * loggingがtrueの場合はログを記録し、falseの場合にはログを記録しない。(<b>Korean</b> : 주어진 List에서
	 * 조건(conditions)에 맞는 첫번째 레코드를 반환한다. 반환된 레코드를 수정하면 원본 레코드가 수정된다. logging이
	 * true일 경우엔 로그를 기록하며 false일 경우에는 로그를 기록하지 않는다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件となる値 (<b>Korean</b> : 검색 조건 값)
	 * @param logging
	 *            ログ記録の有無 (<b>Korean</b> : 로그 기록 유무)
	 * @return
	 */
	public static Map findFirst(final List<Map> list,
			final Conditions conditions, final boolean logging) {
		return _selectFirst(list, conditions, logging, false);
	}

	private static Map _selectFirst(final List<Map> dstList,
			final Conditions conditions, final boolean logging,
			final boolean clone) {
		if (dstList == null)
			return null;
		int idx = indexOf(dstList, conditions);
		Map record = idx != -1 ? dstList.get(idx) : null;
		Map ret = clone ? ListSqlUtil.cloneRecord(record) : record;

		if (logging && LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("selectFirst * from ");
			if (dstList instanceof DataSet)
				sb.append(((DataSet) dstList).getName());
			else
				sb.append("List");
			if (conditions != null)
				sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret;
	}

	private static List<Map> find(final IndexedList indexedList,
			final Conditions conditions) {
		if (indexedList == null)
			return null;
		return indexedList.find(conditions);
	}

	private static Map findFirst(final IndexedList indexedList,
			final Conditions conditions) {
		if (indexedList == null)
			return null;
		return indexedList.findFirst(conditions);
	}

	/**
	 * 与えられたListに条件(conditions)に該当するレコードがあるか有無を確認 (<b>Korean</b> : 주어진 List에
	 * 조건(conditions)에 해당하는 레코드가 있는지 유무 확인)
	 * 
	 * @param dstList
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件となる値 (<b>Korean</b> : 검색 조건 값)
	 * @return
	 */
	public static boolean exists(final List<Map> dstList,
			final Conditions conditions) {
		Map ret = _selectFirst(dstList, conditions, false, false);
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("exists from ");
			if (dstList instanceof DataSet)
				sb.append(((DataSet) dstList).getName());
			else
				sb.append("List");
			sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret != null && ret.size() > 0;
	}

	/**
	 * 与えられたListから条件(conditions)に合うすべてレコードをコピーし、リストに作成して返す。 返されたレコードを修正しても
	 * 原本のレコードは修正されない。 (<b>Korean</b> : 주어진 List에서 조건(conditions)에 맞는 레코드들의 복사본
	 * List를 만들어서 반환한다. 반환된 레코드를 수정해도 원본 레코드는 수정되지 않는다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件となる値 (<b>Korean</b> : 검색 조건 값)
	 * @return
	 */
	public static List<Map> select(final List<Map> list,
			final Conditions conditions) {
		return select(list, conditions, true, true);
	}

	/**
	 * 与えられたListから条件(conditions)に合うすべてレコードのリストを作成し、返す。
	 * 返されたレコードを修正すると原本のレコードも修正される。(<b>Korean</b> : 주어진 List에서 조건(conditions)에
	 * 맞는 레코드들의 List를 만들어서 반환한다. 반환된 레코드를 수정하면 원본 레코드가 수정된다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件となる値 (<b>Korean</b> : 검색 조건 값)
	 * @return
	 */
	public static List<Map> find(final List<Map> list,
			final Conditions conditions) {
		return select(list, conditions, true, false);
	}

	/**
	 * 与えられたListから条件(conditions)に合うすべてレコードのリストを作成し、返す。
	 * 返されたレコードを修正すると原本のレコードも修正される。loggingがtrueの場合にはログを記録し、falseの場合にはログを記録しない。
	 * (<b>Korean</b> : 주어진 List에서 조건(conditions)에 맞는 레코드들의 List를 만들어서 반환한다. 반환된
	 * 레코드를 수정하면 원본 레코드가 수정된다. logging이 true이면 로그를 기록하고 false이면 로그를 기록하지 않는다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件となる値 (<b>Korean</b> : 검색 조건 값)
	 * @param logging
	 *            ログ記録の有無 (<b>Korean</b> : 로그 기록 유무)
	 * @return
	 */
	public static List<Map> find(final List<Map> list,
			final Conditions conditions, boolean logging) {
		return select(list, conditions, logging, false);
	}

	private static List<Map> select(final List<Map> list,
			final Conditions conditions, final boolean logging,
			final boolean clone) {
		List<Map> ret = null;
		String listName = list instanceof DataSet ? ((DataSet) list).getName()
				: null;
		if (listName != null) {
			ret = new WebDataSet(listName);
		} else if (listName == null) {
			ret = new ArrayList<Map>();
		}

		if (list != null) {
			int size = list != null ? list.size() : 0;
			for (int i = 0; i < size; i++) {
				Map row = list.get(i);
				if (conditions == null || conditions.suits(row)) {
					ret.add(clone ? ListSqlUtil.cloneRecord(row) : row);
				}
			}
		}
		if (logging && LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("select * from ");
			if (list instanceof DataSet)
				sb.append(((DataSet) list).getName());
			else
				sb.append("LIST");
			if (conditions != null)
				sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret;
	}

	public static List<Map> innerJoin(final List<Map> leftList,
			final String[] leftSelectKeys, final List<Map> rightList,
			final String[] rightSelectKeys, final JoinKeys joinKeys) {
		return innerJoin(leftList, null, leftSelectKeys, null, rightList, null,
				rightSelectKeys, null, joinKeys);
	}

	public static List<Map> innerJoin(final List<Map> leftList,
			final String leftPrefix, final String[] leftSelectKeys,
			final Conditions leftConditions, final List<Map> rightList,
			final String rightPrefix, final String[] rightSelectKeys,
			Conditions rightConditions, JoinKeys joinKeys) {
		return join(leftList, leftPrefix, leftSelectKeys, leftConditions,
				rightList, rightPrefix, rightSelectKeys, rightConditions,
				joinKeys, false, false);
	}

	/**
	 * leftListを基準にrightListの値を追加したListを作成し、返す。
	 * leftListとrightListをjoinKeysでjoinをする。
	 * rightListから該当するレコードがない場合にもleftListのレコードはすべて取得される。 SQL文のLEFT OUTER
	 * JOINに該当する。返されたレコードを修正しても原本のレコードは修正されない。(<b>Korean</b> : leftList을 기준으로
	 * rightList의 값을 추가한 List를 만들어 반환한다. leftList와 rightList를 joinKeys로 join을
	 * 한다. rightList에서 해당하는 레코드가 없을 경우에도 leftList의 레코드는 모두 취득된다. SQL 문의 LEFT
	 * OUTER JOIN에 해당한다. 반환된 레코드를 수정해도 원본 레코드는 수정되지 않는다.)
	 * 
	 * @param leftList
	 *            Left List&lt;Map&gt;
	 * @param leftSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param rightList
	 *            Right List&lt;Map&gt;
	 * @param rightSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param joinKeys
	 *            Join Keys
	 * @return
	 */
	public static List<Map> leftOuterJoin(final List<Map> leftList,
			final String[] leftSelectKeys, final List<Map> rightList,
			final String[] rightSelectKeys, final JoinKeys joinKeys) {
		return leftOuterJoin(leftList, null, leftSelectKeys, null, rightList,
				null, rightSelectKeys, null, joinKeys, false);
	}

	/**
	 * leftListを基準にrightListの値を追加したListを作成し、返す。
	 * lleftListとrightListをjoinKeysでjoinをする。
	 * rightListから該当するレコードがない場合にもleftListのレコードはすべて取得される。
	 * 但し、rightListでJOINになる一番目のレコードのみ取得する。 SQL文のLEFT OUTER JOINに該当する。
	 * 返されたレコードを修正しても原本のレコードは修正されない。(<b>Korean</b> : leftList을 기준으로 rightList의
	 * 값을 추가한 List를 만들어 반환한다. leftList와 rightList를 joinKeys로 join을 한다.
	 * rightList에서 해당하는 레코드가 없을 경우에도 leftList의 레코드는 모두 취득된다. 단, rightList에서
	 * JOIN이 되는 첫번째 레코드만 취득한다. SQL 문의 LEFT OUTER JOIN에 해당한다. 반환된 레코드를 수정해도 원본
	 * 레코드는 수정되지 않는다.)
	 * 
	 * @param leftList
	 *            Left List&lt;Map&gt;
	 * @param leftSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param rightList
	 *            Right List&lt;Map&gt;
	 * @param rightSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param joinKeys
	 *            Join Keys
	 * @param firstMatchedRightOnly
	 * @return
	 */
	public static List<Map> leftOuterJoin(final List<Map> leftList,
			final String[] leftSelectKeys, final List<Map> rightList,
			final String[] rightSelectKeys, final JoinKeys joinKeys,
			final boolean firstMatchedRightOnly) {
		return leftOuterJoin(leftList, null, leftSelectKeys, null, rightList,
				null, rightSelectKeys, null, joinKeys, firstMatchedRightOnly);
	}

	/**
	 * leftListを基準にrightListの値を追加したListを作成し、返す。
	 * lleftListとrightListをjoinKeysでjoinをする。
	 * rightListから該当するレコードがない場合にもleftListのレコードはすべて取得される。
	 * 但し、rightListでJOINになる一番目のレコードのみ取得する。SQL文のLEFT OUTER JOINに該当する。
	 * 返されたレコードを修正しても原本のレコードは修正されない。(<b>Korean</b> : leftList을 기준으로 rightList의
	 * 값을 추가한 List를 만들어 반환한다. leftList와 rightList를 joinKeys로 join을 한다.
	 * rightList에서 해당하는 레코드가 없을 경우에도 leftList의 레코드는 모두 취득된다. 단, rightList에서
	 * JOIN이 되는 첫번째 레코드만 취득한다. SQL 문의 LEFT OUTER JOIN에 해당한다. 반환된 레코드를 수정해도 원본
	 * 레코드는 수정되지 않는다.)
	 * 
	 * @param leftList
	 *            Left List&lt;Map&gt;
	 * @param leftSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param leftConditions
	 *            leftListに対する取得条件(SQL文のwhere句に該当) (<b>Korean</b> : leftList에 대한
	 *            취득 조건(SQL문의 where 구문에 해당))
	 * @param rightList
	 *            Right List&lt;Map&gt;
	 * @param rightSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param rightConditions
	 *            rightListに対する取得条件(SQL文のwhere句に該当) (<b>Korean</b> : rightList에
	 *            대한 취득 조건(SQL문의 where 구문에 해당))
	 * @param joinKeys
	 *            Join Keys
	 * @param firstMatchedRightOnly
	 * @return
	 */
	public static List<Map> leftOuterJoin(final List<Map> leftList,
			final String[] leftSelectKeys, final Conditions leftConditions,
			final List<Map> rightList, final String[] rightSelectKeys,
			final Conditions rightConditions, final JoinKeys joinKeys,
			final boolean firstMatchedRightOnly) {
		return leftOuterJoin(leftList, null, leftSelectKeys, leftConditions,
				rightList, null, rightSelectKeys, rightConditions, joinKeys,
				firstMatchedRightOnly);
	}

	/**
	 * leftListを基準にrightListの値を追加したListを作成して返す。
	 * leftListとrightListをjoinKeysでjoinする。
	 * rightListで該当するレコードがない場合にもleftListのレコードはすべて取得される。 SQL文のLEFT OUTER
	 * JOINに該当する。返されたレコードを修正しても原本レコードは修正されない。 (<b>Korean</b> : leftList을 기준으로
	 * rightList의 값을 추가한 List를 만들어 반환한다. leftList와 rightList를 joinKeys로 join을
	 * 한다. rightList에서 해당하는 레코드가 없을 경우에도 leftList의 레코드는 모두 취득된다. SQL 문의 LEFT
	 * OUTER JOIN에 해당한다. 반환된 레코드를 수정해도 원본 레코드는 수정되지 않는다.)
	 * 
	 * @param leftList
	 *            Left List&lt;Map&gt;
	 * @param leftPrefix
	 *            取得したleftListカラムのprefix. prefixが "LEFT"の場合、カラム名の前に"LEFT."が付く。
	 *            (<b>Korean</b> : 취득한 leftList 칼럼의 prefix. prefix가 "LEFT"일 경우
	 *            칼럼명 앞에 "LEFT."가 붙는다.)
	 * @param leftSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param leftConditions
	 *            leftListに対する取得条件(SQL文のwhere句に該当) (<b>Korean</b> : leftList에 대한
	 *            취득 조건(SQL문의 where 구문에 해당))
	 * @param rightList
	 *            Right List&lt;Map&gt;
	 * @param rightPrefix
	 *            取得したrightPrefixカラムのprefix. prefixが"LEFT"の場合、カラム名の前に"LEFT."が付く。
	 *            (<b>Korean</b> : 취득한 rightPrefix 칼럼의 prefix. prefix가 "LEFT"일
	 *            경우 칼럼명 앞에 "LEFT."가 붙는다.)
	 * @param rightSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param rightConditions
	 *            rightListに対する取得条件(SQL文のwhere句に該当) (<b>Korean</b> : rightList에
	 *            대한 취득 조건(SQL문의 where 구문에 해당))
	 * @param joinKeys
	 *            Join Keys
	 * @return
	 */
	public static List<Map> leftOuterJoin(final List<Map> leftList,
			final String leftPrefix, final String[] leftSelectKeys,
			final Conditions leftConditions, final List<Map> rightList,
			final String rightPrefix, final String[] rightSelectKeys,
			Conditions rightConditions, JoinKeys joinKeys) {
		return leftOuterJoin(leftList, leftPrefix, leftSelectKeys,
				leftConditions, rightList, rightPrefix, rightSelectKeys,
				rightConditions, joinKeys, false);
	}

	private static String selectKeysToString(final String prefix,
			final String[] selKeys) {
		StringBuilder sb = new StringBuilder();
		if (selKeys == null || selKeys.length == 0) {
			if (!DataUtil.isEmpty(prefix))
				sb.append(prefix).append(".");
			sb.append("*");
		} else {
			for (int i = 0; i < selKeys.length; i++) {
				if (i > 0)
					sb.append(", ");
				if (!DataUtil.isEmpty(prefix))
					sb.append(prefix).append(".");
				sb.append(selKeys[i]);
			}
		}
		return sb.toString();
	}

	/**
	 * leftListを基準にrightListの値を追加したListを作成して返す。
	 * leftListとrightListをjoinKeysでjoinする。
	 * rightListから該当するレコードがない場合にもleftListのレコードはすべて取得される。
	 * 但し、rightListからJOINになる一番目のレコードのみを取得する。SQL文のLEFT OUTER JOINに該当する。
	 * 返されたレコードを修正しても原本のレコードは修正されない。 (<b>Korean</b> : leftList을 기준으로 rightList의
	 * 값을 추가한 List를 만들어 반환한다. leftList와 rightList를 joinKeys로 join을 한다.
	 * rightList에서 해당하는 레코드가 없을 경우에도 leftList의 레코드는 모두 취득된다. 단, rightList에서
	 * JOIN이 되는 첫번째 레코드만 취득한다. SQL 문의 LEFT OUTER JOIN에 해당한다. 반환된 레코드를 수정해도 원본
	 * 레코드는 수정되지 않는다.)
	 * 
	 * @param leftList
	 *            Left List&lt;Map&gt;
	 * @param leftPrefix
	 *            取得したleftListカラムのprefix. prefixが"LEFT"の場合、カラム名の前に"LEFT."が付く。
	 *            (<b>Korean</b> : 취득한 leftList 칼럼의 prefix. prefix가 "LEFT"일 경우
	 *            칼럼명 앞에 "LEFT."가 붙는다.)
	 * @param leftSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param leftConditions
	 *            leftListに対する取得条件(SQL文のwhere句に該当) (<b>Korean</b> : leftList에 대한
	 *            취득 조건(SQL문의 where 구문에 해당))
	 * @param rightList
	 *            Right List&lt;Map&gt;
	 * @param rightPrefix
	 *            取得したrightPrefixカラムのprefix. prefixが"LEFT"の場合、カラム名の前に"LEFT."が付く。
	 *            (<b>Korean</b> : 취득한 rightPrefix 칼럼의 prefix. prefix가 "LEFT"일
	 *            경우 칼럼명 앞에 "LEFT."가 붙는다.)
	 * @param rightSelectKeys
	 *            取得するカラムキー (<b>Korean</b> : 취득할 칼럼 키)
	 * @param rightConditions
	 *            rightListに対する取得条件(SQL文のwhere句に該当) (<b>Korean</b> : rightList에
	 *            대한 취득 조건(SQL문의 where 구문에 해당))
	 * @param joinKeys
	 *            Join Keys
	 * @param firstMatchedRightOnly
	 * @return
	 */
	public static List<Map> leftOuterJoin(final List<Map> leftList,
			final String leftPrefix, final String[] leftSelectKeys,
			final Conditions leftConditions, final List<Map> rightList,
			final String rightPrefix, final String[] rightSelectKeys,
			final Conditions rightConditions, final JoinKeys joinKeys,
			final boolean firstMatchedRightOnly) {
		List<Map> ret = join(leftList, leftPrefix, leftSelectKeys,
				leftConditions, rightList, rightPrefix, rightSelectKeys,
				rightConditions, joinKeys, true, firstMatchedRightOnly);
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("select ");
			sb.append(selectKeysToString(leftPrefix, leftSelectKeys));
			sb.append(", ").append(
					selectKeysToString(rightPrefix, rightSelectKeys));
			sb.append(" from ");
			if (leftList instanceof DataSet)
				sb.append(((DataSet) leftList).getName());
			else
				sb.append("leftList");
			sb.append(" leftOuterJoin ");
			if (rightList instanceof DataSet)
				sb.append(((DataSet) rightList).getName());
			else
				sb.append("rightList");
			sb.append(" on ").append(joinKeys.toString());
			if (leftConditions != null || rightConditions != null) {
				sb.append(" where ");
				if (leftConditions != null)
					sb.append(leftConditions);
				if (rightConditions != null)
					sb.append(rightConditions);
			}
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret;
	}

	private static List<Map> join(List<Map> leftList, String[] leftSelectKeys,
			List<Map> rightList, String[] rightSelectKeys, JoinKeys joinKeys,
			boolean outerJoin) {
		return join(leftList, null, leftSelectKeys, null, rightList, null,
				rightSelectKeys, null, joinKeys, outerJoin, false);
	}

	private static List<Map> join(List<Map> leftList, String[] leftSelectKeys,
			Conditions leftConditions, List<Map> rightList,
			String[] rightSelectKeys, Conditions rightConditions,
			JoinKeys joinKeys, boolean outerJoin) {
		return join(leftList, null, leftSelectKeys, leftConditions, rightList,
				null, rightSelectKeys, rightConditions, joinKeys, outerJoin,
				false);
	}

	/**
	 * 与えられたレコードがnullまたはカラムがないブランクかどうかを確認する。<b>(Korean :</b> 주어진 레코드가 null 또는 칼럼이 없는 빈 것인지를 확인한다.<b>)</b>
	 *
	 * @param record
	 * @return
	 */
	private static boolean isEmptyRecord(Map record) {
		return record == null || record.size() == 0;
	}

	private static List<Map> join(final List<Map> leftList,
			final String leftPrefix, final String[] leftSelectKeys,
			final Conditions leftConditions, final List<Map> rightList,
			final String rightPrefix, final String[] rightSelectKeys,
			final Conditions rightConditions, final JoinKeys joinKeys,
			final boolean outerJoin, final boolean firstMatchedRightOnly) {
		List<Map> list = null;
		String leftName = leftList instanceof DataSet ? ((DataSet) leftList)
				.getName() : null;
		String rightName = rightList instanceof DataSet ? ((DataSet) rightList)
				.getName() : null;
		if (leftName != null && rightName != null) {
			list = new WebDataSet(leftName + "+" + rightName);
		} else if (leftName != null && rightName == null) {
			list = new WebDataSet(leftName + "+rightList");
		} else if (leftName == null && rightName != null) {
			list = new WebDataSet("leftList+" + rightName);
		} else if (leftName == null && rightName == null) {
			list = new ArrayList<Map>();
		}

		List<Map> tmpLeft = leftConditions != null ? select(leftList,
				leftConditions) : leftList;
		List<Map> tmpRight = rightConditions != null ? select(rightList,
				rightConditions) : rightList;
		IndexedList indexedList = null;
		if (joinKeys.isIndexEnabled()) {
			indexedList = new IndexedList(tmpRight, joinKeys.getRightKeys());
		}

		for (int r = 0; tmpLeft != null && r < tmpLeft.size(); r++) {
			Map leftRecord = tmpLeft.get(r);
			if (isEmptyRecord(leftRecord))
				continue;
			Conditions conditions = ListSqlUtil.extractConditions(leftRecord,
					joinKeys);
			if (firstMatchedRightOnly) {
				Map record = null;
				if (indexedList != null) {
					record = indexedList.findFirst(conditions);
				} else {
					record = _selectFirst(tmpRight, conditions, false, false);
				}
				if (record == null) {
					if (outerJoin)
						list.add(ListSqlUtil.cloneRecord(leftRecord));
				} else {
					Map mergedRow = ListSqlUtil.merge(leftRecord, leftPrefix,
							leftSelectKeys, record, rightPrefix,
							rightSelectKeys);
					list.add(mergedRow);
				}
			} else {
				List<Map> rows = null;
				if (indexedList != null) {
					rows = indexedList.find(conditions);
				} else {
					rows = select(tmpRight, conditions, false, false);
				}
				if (rows == null || rows.size() == 0) {
					if (outerJoin)
						list.add(ListSqlUtil.cloneRecord(leftRecord));
				} else {
					int max = rows.size();
					for (int s = 0; s < max; s++) {
						Map rightRecord = rows.get(s);
						Map mergedRow = ListSqlUtil.merge(leftRecord,
								leftPrefix, leftSelectKeys, rightRecord,
								rightPrefix, rightSelectKeys);
						list.add(mergedRow);
					}
				}
			}
		}
		return list;
	}

	/**
	 * 条件テーブル(conditionList)の条件に適合するレコードを返す。返されたレコードを修正しても原本のレコードは
	 * 修正されない。(<b>Korean</b> : 조건 테이블(conditionList)의 조건에 부합하는 레코드들을 반환한다. 반환된
	 * 레코드를 수정해도 원본 레코드는 수정되지 않는다.)
	 * 
	 * @param list
	 *            取得するレコードが存在するテーブル (<b>Korean</b> : 취득할 레코드가 있는 테이블)
	 * @param conditionList
	 *            条件テーブル (<b>Korean</b> : 조건 테이블)
	 * @param joinKeys
	 *            条件となるカラム (<b>Korean</b> : 조건 컬럼들)
	 * @return 条件に適合するレコードリスト (<b>Korean</b> : 조건에 부합하는 레코드 리스트)
	 */
	public static List<Map> selectSuitable(List<Map> list,
			List<Map> conditionList, JoinKeys joinKeys) {
		return selectSuitable(list, conditionList, joinKeys, true, true);
	}

	/**
	 * 条件テーブル(conditionList)の条件に適合するレコードを返す。返されたレコードを修正すると、原本レコードも修正される。
	 * (<b>Korean</b> : 조건 테이블(conditionList)의 조건에 부합하는 레코드들을 반환한다. 반환된 레코드를
	 * 수정하면 원본 레코드가 수정된다.)
	 * 
	 * @param list
	 *            取得するレコードが存在するテーブル (<b>Korean</b> : 취득할 레코드가 있는 테이블)
	 * @param conditionList
	 *            条件テーブル (<b>Korean</b> : 조건 테이블)
	 * @param joinKeys
	 *            条件となるカラム (<b>Korean</b> : 조건 컬럼들)
	 * @return 条件に適合するレコードリスト (<b>Korean</b> : 조건에 부합하는 레코드 리스트)
	 */
	public static List<Map> findSuitable(List<Map> list,
			List<Map> conditionList, JoinKeys joinKeys) {
		return selectSuitable(list, conditionList, joinKeys, true, false);
	}

	/**
	 * 条件テーブル(conditionList)の条件に適していないレコードを返す。返されたレコードを修正しても原本のレコードは修正されない。(<b>
	 * Korean</b> : 조건 테이블(conditionList)의 조건에 부합하지 않는 레코드들을 반환한다. 반환된 레코드를 수정해도
	 * 원본 레코드는 수정되지 않는다.)
	 * 
	 * @param list
	 *            取得するレコードが存在するテーブル (<b>Korean</b> : 취득할 레코드가 있는 테이블)
	 * @param conditionList
	 *            条件テーブル (<b>Korean</b> : 조건 테이블)
	 * @param joinKeys
	 *            条件となるカラム (<b>Korean</b> : 조건 컬럼들)
	 * @return 条件に適していないレコードリスト (<b>Korean</b> : 조건에 부합하지 않는 레코드 리스트)
	 */
	public static List<Map> selectUnsuitable(List<Map> list,
			List<Map> conditionList, JoinKeys joinKeys) {
		return selectSuitable(list, conditionList, joinKeys, false, true);
	}

	/**
	 * 条件テーブル(conditionList)の条件に適していないレコードを返す。返されたレコードを修正すると原本のレコードも修正される。
	 * (<b>Korean</b> : 조건 테이블(conditionList)의 조건에 부합하지 않는 레코드들을 반환한다. 반환된 레코드를
	 * 수정하면 원본 레코드가 수정된다.)
	 * 
	 * @param list
	 *            取得するレコードが存在するテーブル (<b>Korean</b> : 취득할 레코드가 있는 테이블)
	 * @param conditionList
	 *            条件テーブル (<b>Korean</b> : 조건 테이블)
	 * @param joinKeys
	 *            条件となるカラム (<b>Korean</b> : 조건 컬럼들)
	 * @return 条件に適していないレコードリスト (<b>Korean</b> : 조건에 부합하지 않는 레코드 리스트)
	 */
	public static List<Map> findUnsuitable(List<Map> list,
			List<Map> conditionList, JoinKeys joinKeys) {
		return selectSuitable(list, conditionList, joinKeys, false, false);
	}

	private static List<Map> selectSuitable(final List<Map> dstList,
			final List<Map> conditionList, final JoinKeys joinKeys,
			final boolean addSuitable, final boolean clone) {
		List<Map> ret = null;
		String listName = dstList instanceof DataSet ? ((DataSet) dstList)
				.getName() : null;
		if (listName != null) {
			ret = new WebDataSet(listName);
		} else if (listName == null) {
			ret = new ArrayList<Map>();
		}

		int size = dstList != null ? dstList.size() : 0;
		for (int r = 0; r < size; r++) {
			Map record = dstList.get(r);
			Conditions conditions = ListSqlUtil.extractConditions(record,
					joinKeys);
			boolean suitable = exists(conditionList, conditions);
			suitable = addSuitable ? suitable : !suitable;
			if (suitable) {
				ret.add(clone ? ListSqlUtil.cloneRecord(record) : record);
			}
		}
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			if (addSuitable)
				sb.append("selectSuitable");
			else
				sb.append("selectUnsuitable");
			sb.append(" * from ");
			if (dstList instanceof DataSet)
				sb.append(((DataSet) dstList).getName());
			else
				sb.append("List");
			sb.append(" according to conditionList");
			if (conditionList instanceof DataSet)
				sb.append("[").append(((DataSet) conditionList).getName())
						.append("]");
			sb.append(" on ").append(joinKeys.toString());
			LOG.trace(ListSqlUtil.toString(sb.toString(), ret));
		}
		return ret;
	}

	private static void updateLeftByFirstRight(List<Map> leftList,
			String leftKey, List<Map> rightList, String rightKey,
			String[] joinKeys) {
		if (leftList == null || leftList.size() == 0 || rightList == null
				|| rightList.size() == 0)
			return;
		int size = leftList != null ? leftList.size() : 0;
		for (int r = 0; r < size; r++) {
			Map leftRow = leftList.get(r);
			Conditions conditions = ListSqlUtil.extractConditions(leftRow,
					joinKeys);
			Map first = selectFirst(rightList, conditions);
			if (first != null)
				leftRow.put(leftKey, first.get(rightKey));
		}
	}

	private static boolean updateFirst(List<Map> list, Conditions conditions,
			Map updateCols) {
		if (list == null)
			return false;
		int idx = indexOf(list, conditions);
		if (idx != -1) {
			Map row = list.get(idx);
			row.putAll(updateCols);
			return true;
		}
		return false;
	}

	/**
	 * 与えられた条件(conditions)に合うレコードを探し、setRecordで上書きする。(<b>Korean</b> : 주어진
	 * 조건(conditions)에 맞는 레코드를 찾아서 setRecord로 덮어 쓴다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件 (<b>Korean</b> : 검색 조건)
	 * @param setRecord
	 *            新しい値のレコード (<b>Korean</b> : 새로운 값 레코드)
	 * @return
	 */
	public static int update(List<Map> list, Conditions conditions,
			Map setRecord) {
		int cnt = 0;
		int size = list != null ? list.size() : 0;
		for (int i = 0; i < list.size(); i++) {
			Map row = list.get(i);
			if (conditions == null || conditions.suits(row)) {
				row.putAll(setRecord);
				cnt++;
				if (LOG.isTraceEnabled()) {
					StringBuilder sb = new StringBuilder();
					sb.append("update");
					if (list instanceof DataSet)
						sb.append(" from ").append(((DataSet) list).getName());
					if (conditions != null)
						sb.append(" where ").append(conditions);
					LOG.trace(ListSqlUtil.toString(sb.toString(), row));
				}
			}
		}
		return cnt;
	}

	/**
	 * 条件(conditions)に合う一番目のレコードを削除する。 (<b>Korean</b> : 조건(conditions)에 맞는 첫번째
	 * 레코드를 삭제한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件 (<b>Korean</b> : 검색 조건)
	 * @return
	 */
	public static boolean deleteFirst(List<Map> list, Conditions conditions) {
		if (list == null)
			return false;
		int idx = indexOf(list, conditions);
		if (idx != -1) {
			list.remove(idx);
			return true;
		}
		return false;
	}

	/**
	 * 条件(conditions)に合うすべてのレコードを探し、該当のレコードを削除する (<b>Korean</b> :
	 * 조건(conditions)에 맞는 레코드를 모두 찾아서 삭제한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件 (<b>Korean</b> : 검색 조건)
	 * @return
	 */
	public static int delete(List<Map> list, Conditions conditions) {
		int cnt = 0;
		if (list != null) {
			Iterator<Map> it = list.iterator();
			while (it.hasNext()) {
				Map row = it.next();
				if (conditions.suits(row)) {
					it.remove();
					cnt++;
					if (LOG.isTraceEnabled()) {
						StringBuilder sb = new StringBuilder();
						sb.append("delete");
						if (list instanceof DataSet)
							sb.append(" from ").append(
									((DataSet) list).getName());
						if (conditions != null)
							sb.append(" where ").append(conditions);
						LOG.trace(ListSqlUtil.toString(sb.toString(), row));
					}
				}
			}
		}
		return cnt;
	}

	/**
	 * 与えられたカラム(key)の値の前にprefixを付ける。(<b>Korean</b> : 주어진 칼럼(key)의 값의 앞에 prefix를
	 * 붙인다.)
	 * 
	 * @param list
	 * @param key
	 * @param prefix
	 */
	public static void prefixColumn(List<Map> list, String key, String prefix) {
		if (list == null)
			return;
		int size = list != null ? list.size() : 0;
		for (int r = 0; r < size; r++) {
			Map row = list.get(r);
			if (row != null) {
				Object val = row.get(key);
				String str = null;
				if (val == null)
					str = prefix;
				else if (val instanceof String)
					str = prefix + val;
				else
					str = prefix + String.valueOf(val);
				row.put(key, str);
			}
		}
	}

	/**
	 * 与えられたカラム(key)の値の後ろににpostfixを付ける。(<b>Korean</b> : 주어진 칼럼(key)의 값의 뒤에
	 * postfix를 붙인다.)
	 * 
	 * @param list
	 * @param key
	 * @param postfix
	 */
	public static void postfixColumn(List<Map> list, String key, String postfix) {
		if (list == null)
			return;
		int size = list != null ? list.size() : 0;
		for (int r = 0; r < size; r++) {
			Map row = list.get(r);
			if (row != null) {
				Object val = row.get(key);
				String str = null;
				if (val == null)
					str = postfix;
				else if (val instanceof String)
					str = val + postfix;
				else
					str = String.valueOf(val) + postfix;
				row.put(key, str);
			}
		}
	}

	/**
	 * 一番目のレコードから順に取得し、handlerを実行する。 (<b>Korean</b> : 첫번째 레코드부터 차례대로 취득하여
	 * handler를 실행한다.)
	 * 
	 * @param list
	 * @param handler
	 */
	public static void handleRecords(List<Map> list, RecordHandler handler) {
		if (list == null)
			return;
		int size = list != null ? list.size() : 0;
		try {
			for (int r = 0; r < size; r++) {
				Map record = list.get(r);
				if (record != null) {
					handler.handle(r, record);
				}
			}
		} catch (Exception e) {
			if (e instanceof RuntimeException)
				throw (RuntimeException) e;
			else
				throw new RuntimeException(e);
		}
	}

	/**
	 * List&lt;Map&gt;から与えられたキーの値に該当するobjectをIntegerに変換する。 <b>(Korean :</b>
	 * List&lt;Map&gt;에서 주어진 키의 값에 해당하는 object를 Integer로 변환한다.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param key
	 */
	public static void toInt(List<Map> list, final String key) {
		handleRecords(list, new RecordHandler() {
			public void handle(int index, Map record) throws Exception {
				int value = DataUtil.toInt(record.get(key));
				record.put(key, value);
			}
		});
	}

	/**
	 * 一番目のレコードから順に取得して deleterを実行し、deleterの戻り値がtrueのレコードは削除され、
	 * falseのレコードはそのままにする。(<b>Korean</b> : 첫번째 레코드부터 차례대로 취득하여 deleter를 실행하고
	 * deleter의 반환 값이 true인 레코드는 삭제하며 false 레코드는 그냥 둔다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param deleter
	 */
	public static void deleteRecords(List<Map> list, RecordDeleter deleter) {
		if (list == null)
			return;
		List<Map> ret = new ArrayList<Map>();
		int size = list != null ? list.size() : 0;
		for (int r = 0; r < size; r++) {
			Map row = list.get(r);
			if (row != null) {
				if (!deleter.delete(row)) {
					ret.add(row);
				} else {
					if (LOG.isTraceEnabled()) {
						StringBuilder sb = new StringBuilder();
						sb.append("deleteRow");
						if (list instanceof DataSet)
							sb.append(" from ").append(
									((DataSet) list).getName());
						LOG.trace(ListSqlUtil.toString(sb.toString(), row));
					}
				}
			}
		}
		list.clear();
		list.addAll(ret);
	}

	/**
	 * ListのscrKeyの値をdstKeyにコピーする。 (<b>Korean</b> : List의 srcKey의 값을 dstKey로
	 * 복사한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param dstKey
	 *            対象のカラムキー (<b>Korean</b> : 대상 칼럼 키)
	 * @param srcKey
	 *            原本のカラムキー (<b>Korean</b> : 원본 칼럼 키)
	 */
	public static void copyColumn(List<Map> list, String dstKey, String srcKey) {
		copyColumn(list, null, new CopyKeys(dstKey, srcKey), null);
	}

	/**
	 * ListのscrKeyの値をdstKeyにコピーする。 (<b>Korean</b> : List의 srcKey의 값을 dstKey로
	 * 복사한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件 (<b>Korean</b> : 검색 조건)
	 * @param dstKey
	 *            対象のカラムキー (<b>Korean</b> : 대상 칼럼 키)
	 * @param srcKey
	 *            原本のカラムキー (<b>Korean</b> : 원본 칼럼 키)
	 */
	public static void copyColumn(List<Map> list, Conditions conditions,
			String dstKey, String srcKey) {
		copyColumn(list, conditions, new CopyKeys(dstKey, srcKey), null);
	}

	/**
	 * ListのsrcKeyの値をvalueConverterを利用し変換した後、dstKeyカラムにコピーする。 <b>(Korean :</b>
	 * List의 srcKey의 값을 valueConverter를 이용하여 변환 후 dstKey 칼럼으로 복사한다.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param dstKey
	 *            対象のカラムキー (<b>Korean</b> : 대상 칼럼 키)
	 * @param srcKey
	 *            原本のカラムキー (<b>Korean</b> : 원본 칼럼 키)
	 * @param valueConverter
	 *            バリューコンバーター <b>(Korean :</b> 값 변환기. See {@link ValueConverter}
	 *            <b>)</b>
	 */
	public static void copyColumn(List<Map> list, String dstKey, String srcKey,
			ValueConverter valueConverter) {
		copyColumn(list, null, new CopyKeys(dstKey, srcKey), valueConverter);
	}

	/**
	 * ListのsrcKeyの値をvalueConverterを利用し変換後、dstKeyカラムにコピーする。 <b>(Korean :</b>
	 * List의 srcKey의 값을 valueConverter를 이용하여 변환 후 dstKey 칼럼으로 복사한다.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件 (<b>Korean</b> : 검색 조건)
	 * @param dstKey
	 *            対象のカラムキー (<b>Korean</b> : 대상 칼럼 키)
	 * @param srcKey
	 *            原本のカラムキー (<b>Korean</b> : 원본 칼럼 키)
	 * @param valueConverter
	 *            バリューコンバーター <b>(Korean :</b> 값 변환기. See {@link ValueConverter}
	 *            <b>)</b>
	 */
	public static void copyColumn(List<Map> list, Conditions conditions,
			String dstKey, String srcKey, ValueConverter valueConverter) {
		copyColumn(list, conditions, new CopyKeys(dstKey, srcKey),
				valueConverter);
	}

	/**
	 * Listから与えられたconditionsの条件に該当するレコードを検索した後、該当レコードからcopyKeysに該当するカラムをコピーする。
	 * 　　　　 * copyKeysではコピーする原本カラムと対象カラムのキーを指定している。{@link CopyKeys}
	 * 参照してください。<b>(Korean :</b> List에서 주어진 conditions의 조건에 해당하는 레코드를 검색 후 해당
	 * 레코드에서 copyKeys에 해당하는 칼럼들을 복사한다. copyKeys에서는 복사할 원본 칼럼과 대상 칼럼의 키를 지정하고 있다.
	 * {@link CopyKeys} 참조 요망.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件 (<b>Korean</b> : 검색 조건)
	 * @param copyKeys
	 *            {@link CopyKeys}
	 */
	public static void copyColumn(List<Map> list, Conditions conditions,
			CopyKeys copyKeys) {
		copyColumn(list, conditions, copyKeys, null);
	}

	/**
	 * Listから与えられたconditionsの条件に該当するレコードを検索した後、該当レコードからcopyKeysに該当するカラムをコピーする。
	 * 　　　　 * copyKeysではコピーする原本カラムと対象カラムのキーを指定している。{@link CopyKeys} 参照さてください。
	 * <b>(Korean :</b> List에서 주어진 conditions의 조건에 해당하는 레코드를 검색 후 해당 레코드에서
	 * copyKeys에 해당하는 칼럼들을 복사한다. copyKeys에서는 복사할 원본 칼럼과 대상 칼럼의 키를 지정하고 있다.
	 * {@link CopyKeys} 참조 요망.<b>)</b>
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件 (<b>Korean</b> : 검색 조건)
	 * @param copyKeys
	 *            {@link CopyKeys}
	 * @param valueConverter
	 *            バリューコンバーター <b>(Korean :</b> 값 변환기. See {@link ValueConverter#}
	 *            <b>)</b>
	 */
	public static void copyColumn(List<Map> list, Conditions conditions,
			CopyKeys copyKeys, ValueConverter valueConverter) {
		if (list == null || copyKeys == null || copyKeys.size() == 0)
			return;

		StringBuilder sb = new StringBuilder();

		int size = list != null ? list.size() : 0;
		for (int r = 0; r < size; r++) {
			Map record = list.get(r);
			if (record != null) {
				if (conditions == null || conditions.suits(record)) {
					int max = copyKeys.size();
					for (int k = 0; k < max; k++) {
						String srcKey = copyKeys.getSourceKey(k);
						String dstKey = copyKeys.getDestinationKey(k);
						Object srcVal = record.get(srcKey);
						Object dstVal = valueConverter != null ? valueConverter
								.convert(srcVal) : srcVal;
						record.put(dstKey, dstVal);
						if (LOG.isTraceEnabled()) {
							sb.append("copyColumn ").append(srcKey)
									.append(" to ").append(dstKey);
						}
					}
				}
			}
		}
		if (LOG.isTraceEnabled()) {
			if (list instanceof DataSet)
				sb.append(" from ").append(((DataSet) list).getName());
			if (conditions != null)
				sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), list));
		}
	}

	/**
	 * カラムsrcKeyの値がempty(null or "")ではない場合、srcKeyカラムの値をdstKeyカラムにコピーする。
	 * (<b>Korean</b> : 칼럼 srcKey의 값이 empty(null or "")가 아닌 경우 srcKey 칼럼의 값을
	 * dstKey 칼럼에 복사한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param dstKey
	 *            コピー対象のカラムキー (<b>Korean</b> : 복사 대상 칼럼 키)
	 * @param srcKey
	 *            コピー元のカラムキー (<b>Korean</b> : 복사 원본 칼럼 키)
	 */
	public static void copyNotEmptyColumn(List<Map> list, String dstKey,
			String srcKey) {
		if (list == null)
			return;
		int size = list != null ? list.size() : 0;
		for (int r = 0; r < size; r++) {
			Map row = list.get(r);
			if (row != null) {
				Object value = row.get(srcKey);
				if (!StringUtil.isEmpty(value))
					row.put(dstKey, value);
			}
		}
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("copyNotEmptyColumn ").append(srcKey).append(" to ")
					.append(dstKey);
			if (list instanceof DataSet)
				sb.append(" from ").append(((DataSet) list).getName());
			LOG.trace(ListSqlUtil.toString(sb.toString(), list));
		}
	}

	/**
	 * Listの与えられたカラムに該当の値を一括設定する。(<b>Korean</b> : List의 주어진 칼럼에 주어진 값을 일괄 설정한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param key
	 *            カラムキー (<b>Korean</b> : 칼럼 키)
	 * @param value
	 *            値 (<b>Korean</b> : 값)
	 */
	public static void setColumn(List<Map> list, String key, Object value) {
		setColumn(list, null, key, value);
	}

	/**
	 * Listの与えられたカラムに該当の値を一括設定する。(<b>Korean</b> : List의 주어진 칼럼에 주어진 값을 일괄 설정한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param conditions
	 *            検索条件 (<b>Korean</b> : 검색 조건)
	 * @param key
	 *            カラムキー (<b>Korean</b> : 칼럼 키)
	 * @param value
	 *            値 (<b>Korean</b> : 값)
	 */
	public static void setColumn(List<Map> list, Conditions conditions,
			String key, Object value) {
		if (list == null)
			return;
		int size = list != null ? list.size() : 0;
		for (int r = 0; r < size; r++) {
			Map record = list.get(r);
			if (record == null) {
				record = new HashMap();
				list.set(r, record);
			}
			if (conditions == null || conditions.suits(record))
				record.put(key, value);
		}
		if (LOG.isTraceEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("setColumn ").append(key).append(" ").append(value);
			if (list instanceof DataSet)
				sb.append(" from ").append(((DataSet) list).getName());
			if (conditions != null)
				sb.append(" where ").append(conditions);
			LOG.trace(ListSqlUtil.toString(sb.toString(), list));
		}
	}

	/**
	 * Listにおいて与えられたカラムがemptyの場合、(null or "")、与えられた値に設定する。 (<b>Korean</b> :
	 * List의 주어진 칼럼이 empty일 경우(null or "") 주어진 값으로 설정한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param key
	 *            カラムキー (<b>Korean</b> : 칼럼 키)
	 * @param value
	 *            値 (<b>Korean</b> : 값)
	 */
	public static void fillEmptyColumnByValue(List<Map> list, String key,
			Object value) {
		if (list == null)
			return;
		int size = list != null ? list.size() : 0;
		for (int r = 0; r < size; r++) {
			Map row = list.get(r);
			if (row == null) {
				row = new HashMap();
				list.add(r, row);
			}
			Object toValue = row.get(key);
			if (StringUtil.isEmpty(toValue))
				row.put(key, value);
		}
	}

	/**
	 * ListのdstKeyカラムの値がemptyの場合(null or "")、srcKeyカラムの値をdstKeyにコピーする。
	 * (<b>Korean</b> : List의 칼럼 dstKey의 값이 empty일 경우(null or "") srcKey 칼럼의 값을
	 * dstKey에 복사한다.)
	 * 
	 * @param list
	 *            List&lt;Map&gt;
	 * @param dstKey
	 *            対象のカラムキー (<b>Korean</b> : 대상 칼럼 키)
	 * @param srcKey
	 *            原本のカラムキー (<b>Korean</b> : 원본 칼럼 키)
	 */
	public static void fillEmptyColumn(List<Map> list, String dstKey,
			String srcKey) {
		if (list == null)
			return;
		int size = list != null ? list.size() : 0;
		for (int r = 0; r < size; r++) {
			Map row = list.get(r);
			if (row == null) {
				row = new HashMap();
				list.add(r, row);
			}
			Object toValue = row.get(dstKey);
			if (StringUtil.isEmpty(toValue))
				row.put(dstKey, row.get(srcKey));
		}
	}
}