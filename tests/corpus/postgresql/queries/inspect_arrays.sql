-- name: inspect_arrays :many
SELECT
    cardinality(tags) AS tag_count,
    cardinality(score_matrix) AS matrix_count,
    array_ndims('{}'::integer[]) AS empty_array_dimensions,
    array_length('{1,2,3}'::integer[], 2) AS missing_dimension_length,
    array_length(score_matrix, :dimension) AS requested_dimension_length,
    array_lower('{}'::integer[], 1) AS empty_array_lower,
    array_upper('{}'::integer[], 1) AS empty_array_upper,
    array_position('{red,green}'::text[], 'blue') AS missing_item_position,
    array_position('{red,green}'::text[], 'green', :start_position) AS starting_position
FROM public.accounts;
