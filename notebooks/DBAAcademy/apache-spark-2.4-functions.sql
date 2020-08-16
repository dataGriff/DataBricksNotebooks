-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Apache Spark 2.4 Built-in and Higher-Order Functions Examples

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## For array types

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_distinct(array&lt;T&gt;): array&lt;T&gt;
-- MAGIC Removes duplicate values from the given array.

-- COMMAND ----------

SELECT array_distinct(array(1, 2, 3, null, 3));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_intersect(array&lt;T&gt;, array&lt;T&gt;): array&lt;T&gt;
-- MAGIC Returns an array of the elements in the intersection of the given two arrays, without duplicates.

-- COMMAND ----------

SELECT array_intersect(array(1, 2, 3), array(1, 3, 5));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_union(array&lt;T&gt;, array&lt;T&gt;): array&lt;T&gt;
-- MAGIC Returns an array of the elements in the union of the given two arrays, without duplicates.

-- COMMAND ----------

SELECT array_union(array(1, 2, 3), array(1, 3, 5));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_except(array&lt;T&gt;, array&lt;T&gt;): array&lt;T&gt;
-- MAGIC Returns an array of the elements in array1 but not in array2, without duplicates.

-- COMMAND ----------

SELECT array_except(array(1, 2, 3), array(1, 3, 5));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_join(array&lt;String&gt;, String[, String]): String
-- MAGIC Concatenates the elements of the given array using the delimiter and an optional string to replace nulls. If no value is set for null replacement, any null value is filtered.

-- COMMAND ----------

SELECT array_join(array('hello', 'world'), ' ');

-- COMMAND ----------

SELECT array_join(array('hello', null ,'world'), ' ');

-- COMMAND ----------

SELECT array_join(array('hello', null ,'world'), ' ', ',');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_max(array&lt;T&gt;): T
-- MAGIC Returns the maximum value in the given array. null elements are skipped.

-- COMMAND ----------

SELECT array_max(array(1, 20, null, 3));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_min(array&lt;T&gt;): T
-- MAGIC Returns the minimum value in the given array. null elements are skipped.

-- COMMAND ----------

SELECT array_min(array(1, 20, null, 3));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_position(array&lt;T&gt;, T): Long
-- MAGIC Returns the (1-based) index of the first element of the given array as long.

-- COMMAND ----------

SELECT array_position(array(3, 2, 1), 1);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_remove(array&lt;T&gt;, T): array&lt;T&gt;
-- MAGIC Remove all elements that equal to the given element from the given array.

-- COMMAND ----------

SELECT array_remove(array(1, 2, 3, null, 3), 3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### arrays_overlap(array&lt;T&gt;, array&lt;T&gt;): array&lt;T&gt;
-- MAGIC Returns true if array1 contains at least a non-null element present also in array2. If the arrays have no common element and they are both non-empty and either of them contains a null element null is returned, false otherwise.

-- COMMAND ----------

SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_sort(array&lt;T&gt;): array&lt;T&gt;
-- MAGIC Sorts the input array in ascending order. The elements of the input array must be orderable. Null elements will be placed at the end of the returned array.

-- COMMAND ----------

SELECT array_sort(array('b', 'd', null, 'c', 'a'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### concat(String, ...): String / concat(array&lt;T&gt;, ...): array&lt;T&gt;
-- MAGIC Returns the concatenation of col1, col2, ..., colN.
-- MAGIC 
-- MAGIC This function works with strings, binary and compatible array columns.

-- COMMAND ----------

SELECT concat('Spark', 'SQL');

-- COMMAND ----------

SELECT concat(array(1, 2, 3), array(4, 5), array(6));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### flatten(array&lt;array&lt;T&gt;&gt;): array&lt;T&gt;
-- MAGIC Transforms an array of arrays into a single array.

-- COMMAND ----------

SELECT flatten(array(array(1, 2), array(3, 4)));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_repeat(T, Int): array&lt;T&gt;
-- MAGIC Returns the array containing element count times.

-- COMMAND ----------

SELECT array_repeat('123', 2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### reverse(String): String / reverse(array&lt;T&gt;): array&lt;T&gt;
-- MAGIC Returns a reversed string or an array with reverse order of elements.
-- MAGIC 
-- MAGIC Reverse logic for arrays is available since 2.4.0.

-- COMMAND ----------

SELECT reverse('Spark SQL');

-- COMMAND ----------

SELECT reverse(array(2, 1, 4, 3));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### sequence(T, T[, T]): array&lt;T&gt;
-- MAGIC Generates an array of elements from start to stop (inclusive), incrementing by step. The type of the returned elements is the same as the type of argument expressions.

-- COMMAND ----------

SELECT sequence(1, 5);

-- COMMAND ----------

SELECT sequence(5, 1);

-- COMMAND ----------

SELECT sequence(to_date('2018-01-01'), to_date('2018-03-01'), interval 1 month);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### shuffle(array&lt;T&gt;): array&lt;T&gt;
-- MAGIC Returns a random permutation of the given array.

-- COMMAND ----------

SELECT shuffle(array(1, 20, 3, 5));

-- COMMAND ----------

SELECT shuffle(array(1, 20, null, 3));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### slice(array&lt;T&gt;, Int, Int): array&lt;T&gt;
-- MAGIC Subsets the given array starting from index start (or starting from the end if start is negative) with the specified length.

-- COMMAND ----------

SELECT slice(array(1, 2, 3, 4), 2, 2);

-- COMMAND ----------

SELECT slice(array(1, 2, 3, 4), -2, 2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### array_zip(array&lt;T&gt;, array&lt;U&gt;, ...): array&lt;struct&lt;T, U, ...&gt;&gt;
-- MAGIC Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.

-- COMMAND ----------

SELECT arrays_zip(array(1, 2, 3), array(2, 3, 4));

-- COMMAND ----------

SELECT arrays_zip(array(1, 2), array(2, 3), array(3, 4));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## For map types

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### map_form_arrays(array&lt;K&gt;, array&lt;V&gt;): map&lt;K, V&gt;
-- MAGIC Creates a map with a pair of the given key/value arrays. All elements in keys should not be null.

-- COMMAND ----------

SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### map_from_entries(array&lt;struct&lt;K, V&gt;&gt;): map&lt;K, V&gt;
-- MAGIC Returns a map created from the given array of entries.

-- COMMAND ----------

SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### map_concat(map&lt;K, V&gt;, ...): map&lt;K, V&gt;
-- MAGIC Returns the union of all the given maps.

-- COMMAND ----------

SELECT map_concat(map(1, 'a', 2, 'b'), map(3, 'c', 4, 'd'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## For both array and map types

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### element_at(array&lt;T&gt;, Int): T / element_at(map&lt;K, V&gt;, K): V
-- MAGIC For arrays, returns an element of the given array at given (1-based) index. If index &lt; 0, accesses elements from the last to the first. Returns null if the index exceeds the length of the array.
-- MAGIC 
-- MAGIC For maps, returns a value for the given key, or null if the key is not contained in the map.

-- COMMAND ----------

SELECT element_at(array(1, 2, 3), 2);

-- COMMAND ----------

SELECT element_at(map(1, 'a', 2, 'b'), 2);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### cardinality(array&lt;T&gt;): Int / cardinality(map&lt;K, V&gt;): Int
-- MAGIC An alias of size. Returns the size of the given array or a map. Returns -1 if null.

-- COMMAND ----------

SELECT cardinality(array('b', 'd', 'c', 'a'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Higher-order functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### transform(array&lt;T&gt;, function&lt;T, U&gt;): array&lt;U&gt; and transform(array&lt;T&gt;, function&lt;T, Int, U&gt;): array&lt;U&gt;
-- MAGIC Transform elements in an array using the function.
-- MAGIC 
-- MAGIC If there are two arguments for the lambda function, the second argument means the index of the element.

-- COMMAND ----------

SELECT transform(array(1, 2, 3), x -> x + 1);

-- COMMAND ----------

SELECT transform(array(1, 2, 3), (x, i) -> x + i);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### filter(array&lt;T&gt;, function&lt;T, Boolean&gt;): array&lt;T&gt;
-- MAGIC Filter the input array using the given predicate.

-- COMMAND ----------

SELECT filter(array(1, 2, 3), x -> x % 2 == 1);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### aggregate(array&lt;T&gt;, A, function&lt;A, T, A&gt;[, function&lt;A, R&gt;]): R
-- MAGIC Apply a binary operator to an initial state and all elements in the array, and reduces this to a single state. The final state is converted into the final result by applying a finish function.

-- COMMAND ----------

SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x);

-- COMMAND ----------

SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### exists(array&lt;T&gt;, function&lt;T, Boolean&gt;): Boolean
-- MAGIC Test whether a predicate holds for one or more elements in the array.

-- COMMAND ----------

SELECT exists(array(1, 2, 3), x -> x % 2 == 0);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### zip_with(array&lt;T&gt;, array&lt;U&gt;, function&lt;T, U, R&gt;): array&lt;R&gt;
-- MAGIC Merge the two given arrays, element-wise, into a single array using function. If one array is shorter, nulls are appended at the end to match the length of the longer array, before applying function.

-- COMMAND ----------

SELECT zip_with(array(1, 2, 3), array('a', 'b', 'c'), (x, y) -> (y, x));

-- COMMAND ----------

SELECT zip_with(array(1, 2), array(3, 4), (x, y) -> x + y);

-- COMMAND ----------

SELECT zip_with(array('a', 'b', 'c'), array('d', 'e', 'f'), (x, y) -> concat(x, y));